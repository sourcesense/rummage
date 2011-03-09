(ns cemerick.rummage.encoding-test
  (:require [cemerick.rummage.encoding :as enc])
  (:use clojure.test)
  (:import java.util.Date java.net.URL))

(deftest numbers
  (is (thrown? IllegalArgumentException (-> enc/max-abs-integer inc enc/encode-integer)))
  (is (thrown? IllegalArgumentException (-> enc/max-abs-integer - dec enc/encode-integer)))
  
  (doseq [n [0 1 -1 Integer/MAX_VALUE Integer/MIN_VALUE 495 -216 enc/max-abs-integer (- enc/max-abs-integer)
             (Byte. (byte 34))
             (Short. (short -18))]]
    (is (= n (-> n enc/encode-integer enc/decode-integer))))
  
  (doseq [n [0.1 -0.1 Double/MIN_VALUE (- Double/MIN_VALUE) Double/MAX_VALUE]]
    (is (= n (-> n enc/encode-float enc/decode-float)))))

(defn- roundtrip-id
  [encoding & values]
  (let [{:keys [encode-id decode-id]} encoding]
    (doseq [v values]
      (is (= v (-> v encode-id decode-id))))))

(defn- roundtrip
  [encoding & values]
  (let [{:keys [encode decode]} encoding]
    (doseq [[k v :as pair] values]
      (is (= k (-> k encode decode)))
      (is (= pair (->> pair
                    (apply encode)
                    (apply decode)))))))

(deftest all-strings
  (roundtrip-id enc/all-strings "a")
  (roundtrip enc/all-strings ["a" "b"]))

(deftest keyword-strings
  (roundtrip-id enc/keyword-strings "a")
  (roundtrip enc/keyword-strings [:a "b"] [:ns/a "b"]))

(deftest name-typed-values
  (roundtrip-id enc/name-typed-values
    "a" :a :ns/a 42 (Integer. 42) 42.0 -5000.16 (Float. 42.42)
    true false 
    (Date.) (URL. "http://clojure.org"))
  
  (roundtrip enc/name-typed-values
    [:s/string "a"] [:i/integer 42]
    [:z/boolean true] [:f/floating-point 108.6]
    [:U/url (URL. "http://clojure.org")]
    [:D/date (Date.)]))

(deftest fixed-domain-schema
  (let [record {:name "Amy"
                :birthday (Date.)
                :weight 145.5
                :homepage (URL. "http://clojure.org")
                :karma (Integer. 788)}
        encoding (enc/fixed-domain-schema (into {} (for [[k v] record]
                                                     [k (class v)])))]
    (apply roundtrip encoding record)))
