(ns cemerick.rummage.encoding)

;; thought about making this a protocol, but it's all just too concise
;; clear as regular fns

(def all-strings
  {:encode-id str
   :decode-id identity
   :encode (partial map str)
   :decode identity})

(def keyword-keys-string-values
  (assoc all-strings
    :encode (fn [[k v]]
              [(subs (str k) 1)
               (str v)])
    :decode (fn [[k v]]
              [(keyword k) v])))

;; legacy
#_(comment
  (declare decode-sdb-str)

(defn from-sdb-str 
  "Reproduces the representation of the item from a string created by to-sdb-str"
  [s]
  (let [si (.indexOf s ":")
        tag (subs s 0 si)
        str (subs s (inc si))]
    (decode-sdb-str tag str)))

(defn- encode-sdb-str [prefix s]
  (str prefix ":" s))

(defmulti #^{:doc "Produces the representation of the item as a string for sdb"}
  to-sdb-str type)
(defmethod to-sdb-str String [s] (encode-sdb-str "s" s))
(defmethod to-sdb-str clojure.lang.Keyword [k] (encode-sdb-str "k" (name k)))
(defmethod to-sdb-str Integer [i] (encode-sdb-str "i" (encode-integer 10000000000 i)))
(defmethod to-sdb-str Long [n] (encode-sdb-str "l" (encode-integer 10000000000000000000 n)))
(defmethod to-sdb-str java.util.UUID [u] (encode-sdb-str "U" u))
(defmethod to-sdb-str java.util.Date [d] (encode-sdb-str "D" (AmazonSimpleDBUtil/encodeDate d)))
(defmethod to-sdb-str Boolean [z] (encode-sdb-str "z" z))

(defmulti decode-sdb-str (fn [tag s] tag))
(defmethod decode-sdb-str "s" [_ s] s)
(defmethod decode-sdb-str "k" [_ k] (keyword k))
(defmethod decode-sdb-str "i" [_ i] (decode-integer 10000000000 i))
(defmethod decode-sdb-str "l" [_ n] (decode-integer 10000000000000000000 n))
(defmethod decode-sdb-str "U" [_ u] (java.util.UUID/fromString u))
(defmethod decode-sdb-str "D" [_ d] (AmazonSimpleDBUtil/decodeDate d))
(defmethod decode-sdb-str "z" [_ z] (condp = z, "true" true, "false" false)))