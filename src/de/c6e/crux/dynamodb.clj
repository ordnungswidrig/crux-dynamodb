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
           software.amazon.awssdk.services.dynamodb.model.GetItemResponse
           software.amazon.awssdk.services.dynamodb.model.AttributeValue
           software.amazon.awssdk.services.dynamodb.model.AttributeValue$Builder
           software.amazon.awssdk.services.dynamodb.model.PutItemRequest
           software.amazon.awssdk.services.dynamodb.model.PutItemRequest$Builder
           software.amazon.awssdk.services.dynamodb.model.QueryRequest
           software.amazon.awssdk.services.dynamodb.model.QueryResponse
           software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
           software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
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
;; default   |  1 | bgFy... | 1591111570009 |
;; default   |  2 | bgFy... | 1591111576234 |
;;
;; possible improvements
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


(defn last-tx-id [^DynamoDbClient ddb-client ^String table-name ^String partition-name]
  (let [r (-> (QueryRequest/builder)
              (.tableName table-name)
              (.consistentRead true)
              (.keyConditionExpression "#p = :p")
              (.expressionAttributeNames {"#p" "partition"})
              (.expressionAttributeValues {":p" (s partition-name)})
              (.scanIndexForward false)
              (.limit (int 1))
              .build)

        qr ^QueryResponse (.query ddb-client ^QueryRequest r)
        item ^java.util.Map (first (.items qr))
        ^AttributeValue last (some-> item (.get "tx"))]
    (some-> last
            .n
            Integer/parseInt)))

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
                   (log/info "AWS call aborted stopping tx log query")
                   (log/debug "AWS abort exception was" e)
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

(defn insert-tx [^DynamoDbClient ddb-client table-name partition-name tx-events]
  (let [last-id (last-tx-id ddb-client table-name partition-name)
        _ (log/debug "submitting tx, last tx id was" last-id)

        tx-id (inc (or last-id 0))
        tx-time (System/currentTimeMillis)

        item-values {partition-key (s partition-name)
                     sort-key (n tx-id)
                     "tx-time" (n tx-time)
                     "events"  (b (nippy/fast-freeze tx-events))}

        pirb ^PutItemRequest$Builder (PutItemRequest/builder)

        ^PutItemRequest pir (-> pirb
                                (.item item-values)
                                (.tableName table-name)
                                (.conditionExpression (format "attribute_not_exists(%s)" sort-key))
                                .build)]

    (let [r (.putItem ddb-client pir)]
      (log/debug "Put item succeeded" r)
      {::tx/tx-id tx-id
       ::tx/tx-time tx-time})))

(defrecord DynamoDBTxLog [^DynamoDbClient ddb-client ^String table-name ^String partition-name]
  db/TxLog
  (submit-tx [this tx-events]

    ;; determine the last id and use compare-and-set to insert the tx entry and update the
    ;; last id-used entry at the same time.
    (log/debug "submit-tx" :tx-events tx-events)
    (delay
      (try
        (retry-on #(instance? ConditionalCheckFailedException %)
                  20
                  (fn [] (insert-tx ddb-client table-name partition-name tx-events)))

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
