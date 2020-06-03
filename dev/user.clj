(ns user
  (:require [clojure.tools.logging :as log]
            [crux.api :as crux]))

(defmacro with-timed [s & body]
  `(let [t0# (System/currentTimeMillis)]
     (try ~@body
          (finally (log/info "Time segment" ~s (- (System/currentTimeMillis) t0#))))))

(defn run-a-test [node iters]
  (let [id (java.util.UUID/randomUUID)
        d (rand-int 1e6)]
    (doall
     (for [i (range iters)]
       (let [t0 (System/currentTimeMillis)
             tx (crux/submit-tx node [[:crux.tx/put {:crux.db/id id :some-data d :i i}]])]
         (with-timed "await-tx"
           (crux/await-tx node tx))
         (log/info "Iteration" i "of" iters ": TX" tx "arrived, waiting for id " id)
         (let [[r]
               (first (crux/q (crux/db node)
                              {:find '[d] :where '[[e :crux.db/id id]
                                                   [e :some-data d]]
                               :args [{'id id}]}))]
           (if (= r d)
             [:ok {:time (- (System/currentTimeMillis) t0)}]
             [:err {:result r :expected d :id id}])))))))

(defn get-env [n]
  (if-let [v (System/getenv n)]
    v
    (throw (ex-info (format "Environment parameter %s must be set" n)
                    {:env-param n}))))

(defn create-crux-node [& ddb-client]
  (let [s3-bucket     (get-env "CRUX_S3_BUCKET")
        s3-prefix     (get-env "CRUX_S3_PREFIX")
        ddb-table     (or "crux-tx-log" (get-env "CRUX_DDB_TABLE"))
        ddb-partition (get-env "CRUX_DDB_PARTITION")]

    (crux/start-node
     {:crux.node/topology ['de.c6e.crux.dynamodb/topology 'crux.s3/s3-doc-store]
      :crux.dynamodb/table-name ddb-table
      :crux.dynamodb/partition-name ddb-partition
      :crux.dynamodb/client ddb-client
      :crux.s3/bucket s3-bucket  :crux.s3/prefix s3-prefix
      :crux.tx/poll-sleep-duration 1000})))


(defn run-parallel-tests [c iters]
  (let [es (java.util.concurrent.Executors/newFixedThreadPool c)]
    (with-open [node (create-crux-node)]
      (->> (for [t (range c)]
             (.submit es ^Callable #(try (println "Running test" t)
                                         (let [r (run-a-test node iters)]
                                           r)
                                         (catch Exception e (.printStackTrace e)))))
           (map #(.get ^java.util.concurrent.Future %))
           doall
           println))
    (println "Shutting down ES")
    (.shutdown es)
    (println "Waiting for termination")
    (.awaitTermination es 10 java.util.concurrent.TimeUnit/SECONDS)
    (println "ES shut down.")))

(defn -main []
  (println "Running test")
  (run-parallel-tests 10 100)
  #_(with-open [node (create-crux-node)]
      (println "Query result:" (run-a-test node 5))))
