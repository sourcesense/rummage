(ns cemerick.rummage-test
  (:use [cemerick.rummage :as sdb]
    clojure.test
    clojure.contrib.core)
  (:require [cemerick.rummage.encoding :as encoding]))

#_(do
    (System/setProperty "aws.id" "")
    (System/setProperty "aws.secret-key" ""))

; kill the verbose aws logging
(.setLevel (java.util.logging.Logger/getLogger "com.amazonaws")
  java.util.logging.Level/WARNING)

(def client
  (let [id (System/getProperty "aws.id")
        secret-key (System/getProperty "aws.secret-key")]
    (assert (and id secret-key))
    (create-client id secret-key)))

(defn- uuid
  []
  (str (java.util.UUID/randomUUID)))

(def *test-domain-name* nil)

(def test-domain-name-prefix "rummage-test-")

(defn- test-domain-name
  []
  (str test-domain-name-prefix (uuid)))

(defn- test-domain-name?
  [domain-name]
  (.startsWith domain-name test-domain-name-prefix))

(defn- verify-domain-cleanup
  [f]
  (f)
  (doseq [d (filter test-domain-name? (list-domains client))]
    (delete-domain client d)))

(use-fixtures :once verify-domain-cleanup)

(defn- wait-for-condition
  [f desc]
  (let [wait-condition (loop [waiting 0]
                         (cond
                           (f) true
                           (>= waiting 60) false
                           :else (do
                                   (Thread/sleep 1000)
                                   (recur (inc waiting)))))]
    (when-not (is wait-condition desc)
      (throw (IllegalStateException. desc)))
    wait-condition))

(deftest test-domains
  (let [domain-names (->> (repeatedly test-domain-name)
                       (take 10)
                       set)]
    (doseq [dn domain-names]
      (create-domain client dn))
    
    (wait-for-condition #(= domain-names (->> (list-domains client)
                                           (filter test-domain-name?)
                                           set))
      "Domain listing failed")
    
    (let [metadata (domain-metadata client (first domain-names))]
      (is (= domain-metadata-keys (set (keys metadata))))
      (is (every? number? (vals metadata))))
    
    (doseq [dn domain-names] (delete-domain client dn))
    (wait-for-condition #(empty? (filter test-domain-name? (list-domains client)))
      "Domain deletion failed")))

(defmacro defsdbtest
  [name & body]
  `(deftest ~name
     (print '~name ": ") ; lots of sleeping in these tests, give some indication of life
     (flush)
     (time
       (binding [*test-domain-name* (test-domain-name)]
         (is *test-domain-name*)
         (create-domain client *test-domain-name*)
         (try
           ~@body
           (finally
             (delete-domain client *test-domain-name*)))))))

(defsdbtest test-put+get
  (let [config (assoc encoding/all-strings :client client :consistent-read? true)
        item {:sdb/id "foo"
              :a 5
              :b #{"bar" "baz"}
              :c 'j
              623.7 [false :kw]}]
    (put-attrs config *test-domain-name* item)
    
    (is (= {:sdb/id "foo"
            ":a" "5"
            ":b" #{"bar" "baz"}
            ":c" "j"
            "623.7" #{"false" ":kw"}}
          (get-attrs config *test-domain-name* "foo")))
    
    ; selective attribute retrieval
    (is (= {":a" "5" ":c" "j"}
          (select-keys
            (get-attrs config *test-domain-name* (:sdb/id item) ":a" ":c")
            [":a" ":c"])))))

(defsdbtest test-conditional-put
  (let [config (assoc encoding/all-strings :client client :consistent-read? true)]
    (put-attrs config *test-domain-name* {:sdb/id "foo" :a 5})
    (put-attrs config *test-domain-name* {:sdb/id "foo" :b 6} :not-expecting :c)
    
    (is (thrown-with-msg? com.amazonaws.AmazonServiceException #".*Conditional check failed.*"
          (put-attrs config *test-domain-name* {:sdb/id "foo" :b 7} :not-expecting :b)))
    
    (put-attrs config *test-domain-name* {:sdb/id "foo" :b 9} :expecting [:b "6"])
    (is (= "9" (get (get-attrs config *test-domain-name* "foo" :b) ":b")))
    
    (is (thrown-with-msg? com.amazonaws.AmazonServiceException #".*Conditional check failed.*"
          (put-attrs config *test-domain-name* {:sdb/id "foo" :b 9} :expecting [:b "12"])))))

(defsdbtest test-put-replace
  (let [config (assoc encoding/all-strings :client client :consistent-read? true)]
    (put-attrs config *test-domain-name* {:sdb/id "foo" :a 5})
    (put-attrs config *test-domain-name* {:sdb/id "foo" :a 6} :add-to? #{:a})
    (is (= #{"5" "6"}
          (get (get-attrs config *test-domain-name* "foo") ":a")))))

(defsdbtest test-inconsistent-read
  (let [config (assoc encoding/all-strings :client client)
        domain-name *test-domain-name*]
    (wait-for-condition
      (fn []
        (loop [[id & keys] (->> (repeatedly rand)
                             (map str)
                             (take 100))]
          (when id
            (put-attrs config domain-name {:sdb/id id :key id})
            (if (get-attrs config domain-name id)
              (recur keys)
              true))))
      "Inconsistent read was never inconsistent")))

(defsdbtest test-consistent-read
  (let [config (assoc encoding/all-strings :client client :consistent-read? true)]
    (doseq [id (map str (range 250))]
      (put-attrs config *test-domain-name* {:sdb/id id :key id})
      (when-not (is (get-attrs config *test-domain-name* id))
        (throw (IllegalStateException. (str "Consistent read wasn't on item " id)))))))

(defsdbtest test-batch-put
  (let [config (assoc encoding/all-strings :client client :consistent-read? true)]
    (batch-put-attrs config *test-domain-name* (for [x (range 250)]
                                               {:sdb/id x :key x}))
    (doseq [id (map str (range 250))]
      (is (get-attrs config *test-domain-name* id)))))

(defsdbtest test-delete
  (let [config (assoc encoding/all-strings :client client :consistent-read? true)]
    (put-attrs config *test-domain-name* {:sdb/id "foo" :a 5 :b [5 6 7] :c 7})
    
    (is (thrown-with-msg? com.amazonaws.AmazonServiceException #".*Conditional check failed.*"
          (delete-attrs config *test-domain-name* "foo" :attrs {:c 7} :not-expecting :a)))
    
    (is (thrown-with-msg? com.amazonaws.AmazonServiceException #".*Conditional check failed.*"
          (delete-attrs config *test-domain-name* "foo" :attrs {:c 7} :expecting [:c 8])))
    
    (delete-attrs config *test-domain-name* "foo" :attrs #{:c} :expecting [:c 7])
    (is (nil? (get (get-attrs config *test-domain-name* "foo" :c) ":c")))
    
    (delete-attrs config *test-domain-name* "foo" :attrs {:b 7})    
    (is (= #{"5" "6"} (get (get-attrs config *test-domain-name* "foo" :b) ":b")))
    
    (delete-attrs config *test-domain-name* "foo" :attrs #{:b})    
    (is (nil? (get (get-attrs config *test-domain-name* "foo" :b) ":b")))
    
    (delete-attrs config *test-domain-name* "foo")
    (is (nil? (get-attrs config *test-domain-name* "foo")))))

(defsdbtest test-batch-delete
  (let [config (assoc encoding/all-strings :client client :consistent-read? true)]
    (batch-put-attrs config *test-domain-name* (for [x (range 235)]
                                                 {:sdb/id x :key x :otherkey (inc x)}))
    (is (get-attrs config *test-domain-name* "34"))
    
    (batch-delete-attrs config *test-domain-name*
      (into {} (for [x (range 235)]
                 [x (if (even? x)
                      #{:key :otherkey}
                      {:key x})])))
    
    (doseq [x (range 10)
            :let [item (get-attrs config *test-domain-name* x)]]
      (if (even? x)
        (is (nil? item))
        (is (= (str (inc x)) (get item ":otherkey")))))))
