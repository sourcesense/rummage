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

(deftest test-all-strings
  (let [{:keys [encode decode encode-id decode-id]} enc/all-strings]
    (is (= "a" (encode-id "a")))
    (is (= "a" (decode-id "a")))
    (is (= "a" (encode "a")))
    (is (= "a" (decode "a")))
    (is (= ["a" "b"] (encode "a" "b")))
    (is (= ["a" "b"] (decode "a" "b")))))