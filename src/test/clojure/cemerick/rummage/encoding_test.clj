(ns cemerick.rummage.encoding-test
  (:use cemerick.rummage.encoding
    clojure.test))

(deftest numbers
  (is (thrown? IllegalArgumentException (-> max-abs-integer inc encode-integer)))
  (is (thrown? IllegalArgumentException (-> max-abs-integer - dec encode-integer)))
  
  (doseq [n [0 1 -1 Integer/MAX_VALUE Integer/MIN_VALUE 495 -216 max-abs-integer (- max-abs-integer)
             (Byte. (byte 34))
             (Short. (short -18))]]
    (is (= n (-> n encode-integer decode-integer))))
  
  (doseq [n [0.1 -0.1 Double/MIN_VALUE (- Double/MIN_VALUE) Double/MAX_VALUE]]
    (is (= n (-> n encode-float decode-float)))))