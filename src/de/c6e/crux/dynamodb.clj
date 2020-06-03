(ns de.c6e.crux.dynamodb

  (:require [clojure.tools.logging :as log]
            [crux.node :as n]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.status :as status]
            [crux.tx :as tx]
            [taoensso.nippy :as nippy])
  (:import software.amazon.awssdk.services.dynamodb.DynamoDbClient
           software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
           software.amazon.awssdk.services.dynamodb.model.TableDescription
           software.amazon.awssdk.services.dynamodb.model.GetItemRequest
           software.amazon.awssdk.services.dynamodb.model.GetItemResponse
           software.amazon.awssdk.services.dynamodb.model.GetItemRequest$Builder
           software.amazon.awssdk.services.dynamodb.model.Put
           software.amazon.awssdk.services.dynamodb.model.AttributeValue
           software.amazon.awssdk.services.dynamodb.model.AttributeValue$Builder
           software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
           software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
           software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest$Builder
           software.amazon.awssdk.services.dynamodb.model.QueryRequest
           software.amazon.awssdk.services.dynamodb.model.QueryResponse
           software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
           software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
           software.amazon.awssdk.services.dynamodb.model.TransactionConflictException
           software.amazon.awssdk.core.exception.SdkClientException
           software.amazon.awssdk.core.exception.AbortedException
           software.amazon.awssdk.core.SdkBytes))

;; todo create table code
;;
;; For this to work create table with hash key "partition" (string) and sort-key "tx" (numeric)

(def partition-key "partition")
(def sort-key "tx")

;; The tx log implemented here uses a constant "partition" value which would allow to use multiple
;; instances on the same dynamodb table. The sort-key "tx" is used to store the tx id.
;;
;; Example-data
;;
;; partition | tx | events  | tx-time       | last
;; default   | -1 |         |               | 2
;; default   |  1 | bgFy... | 1591111570009 |
;; default   |  2 | bgFy... | 1591111576234 |
;;
;; The entry at tx "-1" is used to store that last used tx-id. This might be redundant. And be
;; replaced by a reverse sort on tx.

;; possible improvements
;; - drop "last-id-used" value at "-1" in favour of query for the last and CAS in tx-id existence
;; - shard id over partitions to enhance performance but scanning for next tx could be tricky

(set! *warn-on-reflection* true)

;; some helper functions around AttributeValue to help with relfection warnings

(defn ^AttributeValue s [s]
  (-> ^AttributeValue$Builder (AttributeValue/builder) (.s s) .build))

(defn ^AttributeValue n [n]
  (-> ^AttributeValue$Builder (AttributeValue/builder) (.n (str n)) .build))

(defn ^AttributeValue b [byte-array]
  (-> (AttributeValue/builder) (.b (SdkBytes/fromByteArray byte-array)) .build))

(defn ^Long get-av-long [^GetItemResponse i ^String n]
  (let [^AttributeValue av (.get (.item i) n)]
    (Long/parseLong (.n av))))

(defn ^"[B" get-av-byte-array [^GetItemResponse i ^String n]
  (let [^AttributeValue av (.get (.item i) n)
        ^SdkBytes sdk-bytes (.b av)]
    (.asByteArray sdk-bytes)))


;; determine the last tx id used in the partition. Uses the store last-id value
(defn last-tx-id [^DynamoDbClient ddb-client ^String table-name ^String partition-name]
  (try
    (let [r (-> ^GetItemRequest$Builder
                (GetItemRequest/builder) (.key {partition-key (s partition-name)
                                                sort-key (n -1)})
                (.tableName table-name) .build)
          ^GetItemResponse item (.getItem ddb-client ^GetItemRequest r)
          ^AttributeValue last (-> item .item (.get "last"))]
      (some-> last
              .n
              Integer/parseInt))
    (catch ResourceNotFoundException _ nil)))

;; query for all tx after last-tx-id and return a lazyseq. Maybe also could use DynamoDbClient#queryPaginator
;; which directly returns a Iterable. The function takes an atom as a "close-flag" to signal the request
;; to stop the iteration.

(defn query-tx-table [^DynamoDbClient ddb-client close-flag table-name partition-name last-tx-id]
  (letfn [(q* [start-key]
            (when-not @close-flag
              (lazy-seq
               (try
                 (let [q (-> (QueryRequest/builder)
                             (.tableName table-name)
                             (.consistentRead true)
                             (.keyConditionExpression "#p = :p AND #tx > :tx")
                             (.expressionAttributeNames {"#p" "partition" "#tx" "tx"})
                             (.expressionAttributeValues {":p" (s partition-name)
                                                          ":tx" (n (or last-tx-id 0))}))
                       q (cond-> q
                           start-key (.exclusiveStartKey start-key)
                           :aways .build)


                       _ (log/debug "Query" q "last-tx-id" last-tx-id)

                       r ^QueryResponse (.query ddb-client ^QueryRequest q)

                       items (->> (.items r)
                                  (mapv (fn [^java.util.Map i]
                                          {:crux.tx/tx-id (Long/parseLong (.n ^AttributeValue (get i "tx")))
                                           :crux.tx/tx-time (java.util.Date. (Long/parseLong (.n ^AttributeValue (get i "tx-time"))))
                                           :crux.tx.event/tx-events
                                           (nippy/fast-thaw (.asByteArray (.b ^AttributeValue (get i "events"))))
                                           #_(nippy/fast-thaw (get-av-byte-array i "events"))})))
                       last-key (.lastEvaluatedKey r)]

                   (log/debug "Got" (count items) "tems: " (take 2 items))
                   (log/debug "Last-Key" last-key)

                   (if (empty? last-key)
                     items
                     (concat items (q* last-key))))
                 (catch SdkClientException e
                   (log/warn "Got an unexpected AWS SDK exception, closing iterator" e)
                   ;; return and thus stop lazy-seq'ing
                   [])
                 (catch AbortedException e
                   (log/info "AWS call aborted stopping tx log query.")
                   ;; return and thus stop lazy-seq'ing
                   [])))))]
    (q* nil)))

(defn retry-on [pred max f]
  (loop [i 0]
    (let [[e r]
          (try [nil (f)]
               (catch Exception e
                 (if (pred e)
                   [e nil]
                   (throw e))))]
      (if r
        (do
          (log/debug "Success on attempt no" i)
          r)
        (if (< i max)
          (do (log/debug "Failed call, retrying because" e)
              (recur (inc i)))
          (throw (Exception. (format "Giving up after %s attempts." max) e)))))))

(defrecord DynamoDBTxLog [^DynamoDbClient ddb-client ^String table-name ^String partition-name]
  db/TxLog
  (submit-tx [this tx-events]

    ;; determine the last id and use compare-and-set to insert the tx entry and update the
    ;; last id-used entry at the same time.
    (log/debug "submit-tx" :tx-events tx-events)
    (delay
      (try
        (retry-on #(instance? TransactionCanceledException %)
                  20
                  (fn []
                    (let [last-id (last-tx-id ddb-client table-name partition-name)
                          _ (log/debug "submitting tx, last tx id was" last-id)

                          tx-id (inc (or last-id 0))
                          tx-time (System/currentTimeMillis)

                          item-values {partition-key (s partition-name)
                                       sort-key  (n tx-id)
                                       "tx-time" (n tx-time)
                                       "events"       (b (nippy/fast-freeze tx-events))}

                          ^Put tx-put-items (-> (Put/builder) (.tableName table-name) (.item item-values) .build)

                          ^Put tx-update-id (cond-> (-> (Put/builder)
                                                        (.tableName table-name)
                                                        (.item {"partition" (s partition-name)
                                                                "tx"        (n -1)
                                                                "last"      (n tx-id)}))
                                              last-id (.conditionExpression "#last = :lastid")
                                              last-id (.expressionAttributeValues {":lastid" (n last-id)})
                                              last-id (.expressionAttributeNames {"#last" "last"})
                                              :always  .build)

                          ^TransactWriteItem items-wi (-> (TransactWriteItem/builder)
                                                          (.put tx-put-items)
                                                          .build)

                          ^TransactWriteItem update-id-wi (-> (TransactWriteItem/builder)
                                                              (.put tx-update-id)
                                                              .build)

                          ^java.util.Collection twis [items-wi update-id-wi]

                          twir (-> ^TransactWriteItemsRequest$Builder
                                   (TransactWriteItemsRequest/builder)
                                   (.transactItems twis)
                                   .build)]
                      (let [r (.transactWriteItems ddb-client ^TransactWriteItemsRequest twir)]
                        (log/debug "Transaction succeded" r)
                        {::tx/tx-id tx-id
                         ::tx/tx-time tx-time}))))
        (catch ResourceNotFoundException e
          (throw (ex-info "DynamoDb table not found" {:table-name table-name} e))))))

  (open-tx-log [this after-tx-id]
    (log/debug "Opening tx-log" :after-tx-id after-tx-id)
    (let [close-flag (atom nil)]
      ;; use the atom to signal that the cursor is closed and make the query stop iterating
      (cio/->cursor (fn [] (reset! close-flag :close) )
                    (query-tx-table ddb-client close-flag table-name partition-name after-tx-id))))

  (latest-submitted-tx [this]
    {:crux.tx/tx-id (last-tx-id ddb-client table-name partition-name)})

  status/Status
  (status-map [_]
    {:crux.dynamodb/table-info
     (try
       (let [r (-> (DescribeTableRequest/builder) (.tableName table-name) .build)
             table-info ^TableDescription (-> (.describeTable ddb-client ^DescribeTableRequest r) .table)]
         {:status (keyword (.tableStatusAsString table-info))
          :item-count (.itemCount table-info)
          :table-size (.tableSizeBytes table-info)})
       (catch Exception e
         (log/debug e "Could not describe dynamodb table")
         false))}))


;; partition name is the table hash key
(def tx-log
  {:start-fn (fn [{::keys []}
                  {:crux.dynamodb/keys [client table-name partition-name] :or {partition-name "default"}}]
               (assert table-name)
               (log/debug "Creating DynamoDBTxLog" :table-name table-name :partition-name partition-name)
               (->DynamoDBTxLog (or client (DynamoDbClient/create)) table-name partition-name))})

(def topology
  (merge n/base-topology
         {:crux.node/tx-log `tx-log}))
