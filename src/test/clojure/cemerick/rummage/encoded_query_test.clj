(ns cemerick.rummage.encoded-query-test
  (:use [cemerick.rummage :as sdb]
    [cemerick.rummage-test :as test :only (*test-domain-name* defsdbtest client)]
    [cemerick.rummage.query-test :only (dataset)]
    clojure.test)
  (:require [cemerick.rummage.encoding :as enc]))

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