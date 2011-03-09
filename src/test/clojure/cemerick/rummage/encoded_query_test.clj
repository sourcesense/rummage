(ns cemerick.rummage.encoded-query-test
  (:use [cemerick.rummage :as sdb]
    [cemerick.rummage-test :as test :only (*test-domain-name* defsdbtest client)]
    [cemerick.rummage.query-test :only (dataset)]
    clojure.test)
  (:require [cemerick.rummage.encoding :as enc])
  (:import java.net.URL java.util.Date))

(use-fixtures :once test/verify-domain-cleanup)

(defsdbtest keyword-strings
  (let [config (assoc enc/keyword-strings :client client :consistent-read? true)]
    (batch-put-attrs config *test-domain-name* dataset)
    
    (is (= (->> dataset (mapcat keys) set)
          (->> (query config `{select * from ~*test-domain-name*})
            (mapcat keys)
            set)))
    
    (is (= (->> dataset
             (filter #(> (:year %) 1960))
             (map #(select-keys % [:author :title :sdb/id]))
             set)
          (set (query config `{select [:author :title] from ~*test-domain-name* where (> :year 1960)}))))))

(defsdbtest name-typed-values
  (let [config (assoc enc/name-typed-values :client client :consistent-read? true)
        dataset [{:sdb/id (Long. (long 1)) :s/name "Andy" :z/banned false :i/age (Long. (long 44)) :D/joined (java.util.Date.)
                  :f/weight 185.2 :U/link (java.net.URL. "http://aws.amazon.com")}
                 {:sdb/id :ns/foo :s/name "Graham" :z/banned true :i/age (Long. (long 17)) :D/joined (java.util.Date. (long 0))
                  :f/weight 166.0 :U/link (java.net.URL. "http://github.com")}]]
    (batch-put-attrs config *test-domain-name* dataset)
    
    (is (= (set dataset) (set (query config `{select * from ~*test-domain-name*}))))
    
    (is (= (->> dataset
             (filter #(> (:i/age %) 20))
             (map #(select-keys % [:s/name :sdb/id]))
             set)
          (set (query config `{select [:s/name] from ~*test-domain-name* where (> :i/age 20)}))))))

(defsdbtest fixed-domain-schema
  (let [dataset [{:sdb/id :amy :name "Amy" :birthday (Date. (long 5000)) :weight 145.5
                  :homepage (URL. "http://clojure.org") :karma (Integer. 788)}
                 {:sdb/id :tremont :name "Tremont" :karma (Integer. 0) :homepage (URL. "http://www.mfa.org")}
                 {:sdb/id :newton :name "Newton" :karma (Integer. 88) :birthday (Date. (long 10000))
                  :homepage (map #(URL. %) ["http://apple.com" "http://deviantart.com" "http://www.mfa.org"])}]
        encoding (enc/fixed-domain-schema {:name String
                                           :birthday Date
                                           :weight Float
                                           :homepage URL
                                           :karma Integer})
        config (assoc encoding :client client :consistent-read? true)]
    (batch-put-attrs config *test-domain-name* dataset)
    
    (are [expected select] (= (->> expected (map :sdb/id) set)
                            (set (query config select)))
      (filter (comp pos? :karma) dataset)
      `{select id from ~*test-domain-name* where (> :karma 0)}
      
      (filter (comp (partial some #{(URL. "http://www.mfa.org")}) set as-collection :homepage) dataset)
      `{select id from ~*test-domain-name* where (like :homepage "%mfa.org")}
      
      (filter :birthday dataset)
      `{select id from ~*test-domain-name* where (not-null :birthday)}
      
      (->> dataset
        (filter :birthday)
        (filter #(< 7500 (.getTime (:birthday %)) 20000)))
      `{select id from ~*test-domain-name* where (between :birthday ~(Date. (long 7500)) ~(Date. (long 20000)))})
    
    (is (= (->> (filter :birthday dataset)
             (sort-by :birthday)
             (map :sdb/id))
          (query config `{select id from ~*test-domain-name* where (not-null :birthday) order-by [:birthday]})))
    )
  )