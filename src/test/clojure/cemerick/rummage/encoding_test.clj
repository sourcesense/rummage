(ns cemerick.rummage.encoding-test
  (:require [cemerick.rummage.encoding :as enc])
  (:use clojure.test))

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
  (let [{:keys [encode-id decode-id]} enc/all-strings]
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