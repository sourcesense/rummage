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
                           (>= waiting 120) false
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
     (println '~name) ; lots of sleeping in these tests, give some indication of life
     (binding [*test-domain-name* (test-domain-name)]
       (is *test-domain-name*)
       (create-domain client *test-domain-name*)
       (try
         ~@body
         (finally
           (delete-domain client *test-domain-name*))))))

(defsdbtest test-put+get
  (let [config (assoc encoding/all-strings :client client :consistent-read? true)
        item {:sdb/id "foo"
              :a 5
              :b #{"bar" "baz"}
              :c 'j
              623.7 [false :kw]}]
    (put-attrs config *test-domain-name* item)
    
    (let [i2 (get-attrs config *test-domain-name* (:sdb/id item))]
      (is (= "5" (i2 ":a")))
      (is (= (:b item) (i2 ":b")))
      (is (= "j" (i2 ":c")))
      (is (= #{"false" ":kw"} (get i2 "623.7")))
      (is (= "foo" (:sdb/id i2))))
    
    ; selective attribute retrieval
    (is (= {":a" "5" ":c" "j"}
          (select-keys
            (get-attrs config *test-domain-name* (:sdb/id item) ":a" ":c")
            [":a" ":c"])))))



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
