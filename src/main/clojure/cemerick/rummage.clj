;   Copyright (c) Chas Emerick, Rich Hickey, and other contributors. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns cemerick.rummage
 (:import
   cemerick.rummage.DataUtils
   (com.amazonaws.services.simpledb AmazonSimpleDBClient)
   (com.amazonaws.services.simpledb.model CreateDomainRequest DeleteDomainRequest
     ListDomainsRequest
     DomainMetadataRequest)))

(defn create-client
  "Creates a client for talking to a specific AWS SimpleDB
  account. The same client can be reused for multiple requests."
  ([id secret-key]
    (create-client id secret-key (com.amazonaws.ClientConfiguration.)))
  ([id secret-key client-config]
    (AmazonSimpleDBClient. (com.amazonaws.auth.BasicAWSCredentials. id secret-key)
      (.withUserAgent client-config "Rummage - SDB for Clojure"))))

(defn create-domain
  "Creates a domain with the specified name.  Returns successfully if the domain
   already exists."
  [^AmazonSimpleDBClient client name]
  (.createDomain client (CreateDomainRequest. name)))

(defn delete-domain
  "Deletes the named domain."
  [^AmazonSimpleDBClient client name]
  (.deleteDomain client (DeleteDomainRequest. name)))

(defn- list-domains*
  [^AmazonSimpleDBClient client next-token]
  (let [req (-> (ListDomainsRequest.) (.withNextToken next-token))
        res (.listDomains client req)]
    (concat (.getDomainNames res)
      (when (.getNextToken res)
        (list-domains* client (.getNextToken res))))))

(defn list-domains
  "Returns a sequence of all domain names available from the given client."
  [client]
  (list-domains* client nil))

(defn domain-metadata 
  "Returns a map of domain metadata"
  [^AmazonSimpleDBClient client domain]
  (select-keys
    (-> client
      (.domainMetadata (DomainMetadataRequest. domain))
      bean)
    [:timestamp :attributeValuesSizeBytes  :attributeNameCount  :itemCount
     :attributeValueCount  :attributeNamesSizeBytes :itemNamesSizeBytes]))

#_(comment (declare decode-sdb-str)

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
(defmethod decode-sdb-str "z" [_ z] (condp = z, "true" true, "false" false))

(defn- item-attrs [item]
  (reduce (fn [kvs [k v :as kv]]
              (cond
               (= k :sdb/id) kvs
               (set? v) (reduce (fn [kvs v] (conj kvs [k v])) kvs v)
               :else (conj kvs kv)))
          [] item))

(defn item-triples
  "Given an item-map, returns a set of triples representing the attrs of an item"
  [item]
  (let [s (:sdb/id item)]
    (reduce (fn [ts [k v]]
                (conj ts {:s s :p k :o v}))
            #{} (item-attrs item))))

(defn- replaceable-attrs [item add-to?]
  (map (fn [[k v]] (ReplaceableAttribute. (to-sdb-str k) (to-sdb-str v) (not (add-to? k))))
                  (item-attrs item)))

(defn put-attrs
  "Puts attrs for one item into the domain. By default, attrs replace
  all values present at the same attrs/keys. You can pass an add-to?
  function (usually a set), and when it returns true for a key, values
  will be added to the set of values at that key, if any."
  ([client domain item] (put-attrs client domain item #{}))
  ([client domain item add-to?]
    (let [item-name (to-sdb-str (:sdb/id item))
          attrs (replaceable-attrs item add-to?)]
      (.putAttributes client (PutAttributesRequest. domain item-name attrs)))))

(defn batch-put-attrs
  "Puts the attrs for multiple items into a domain, with the same semantics as put-attrs"
  ([client domain items] (batch-put-attrs client domain items #{}))
  ([client domain items add-to?]
    (.batchPutAttributes client
      (BatchPutAttributesRequest. domain
        (map
          #(ReplaceableItem. (to-sdb-str (:sdb/id %)) (replaceable-attrs % add-to?))
          items)))))

(defn setify
  "If v is a set, returns it, else returns a set containing v"
  [v]
  (if (set? v) v (hash-set v)))

(defn- build-item [item-id attrs]
  (reduce (fn [m #^Attribute a]
            (let [k (from-sdb-str (.getName a))
                  v (from-sdb-str (.getValue a))
                  ov (m k)]
              (assoc m k (if ov (conj (setify ov) v) v))))
          {:sdb/id item-id} attrs))

(defn get-attrs
  "Gets the attributes for an item, as a valid item map. If no attrs are supplied,
  gets all attrs for the item."
  [client domain item-id & attrs]
  (let [r (.getAttributes client
                          (GetAttributesRequest. domain (to-sdb-str item-id) (map to-sdb-str attrs)))
        attrs (.. r getGetAttributesResult getAttribute)]
    (build-item item-id attrs)))

;todo remove a subset of a set of vals
(defn delete-attrs
  "Deletes the attrs from the item. If no attrs are supplied, deletes
  all attrs and the item. attrs can be a set, in which case all values
  at those keys will be deleted, or a map, in which case only the
  values supplied will be deleted."
  ([client domain item-id] (delete-attrs client domain item-id #{}))
  ([client domain item-id attrs]
    (.deleteAttributes client
      (DeleteAttributesRequest. domain (to-sdb-str item-id)
        (cond
          (set? attrs) (map #(Attribute. (to-sdb-str %) nil) attrs)
          (map? attrs) (map (fn [[k v]] (Attribute. (to-sdb-str k) (to-sdb-str v))) attrs)
          :else (throw (Exception. "attrs must be set or map")))))))

(defn- attr-str [attr]
  (if (sequential? attr)
    (let [[op a] attr]
      (assert (= op 'every))
      (format "every(%s)" (attr-str a)))
    (str \` (to-sdb-str attr) \`)))

(defn- op-str [op]
  (.replace (str op) "-" " "))

(defn- val-str [v]
  (str \" (.replace (to-sdb-str v) "\"" "\"\"") \"))

(defn- simplify-sym [x]
  (if (and (symbol? x) (namespace x))
    (symbol (name x))
    x))

(defn- expr-str
  [e]
  (condp #(%1 %2) (simplify-sym (first e))
    '#{not}
      (format "(not %s)" (expr-str (second e)))
    '#{and or intersection}
      :>> #(format "(%s %s %s)" (expr-str (nth e 1)) % (expr-str (nth e 2)))
    '#{= != < <= > >= like not-like}
      :>> #(format "(%s %s %s)" (attr-str (nth e 1)) (op-str %) (val-str (nth e 2)))
    '#{null not-null}
      :>> #(format "(%s %s)" (op-str %) (attr-str (nth e 1)))
    '#{between}
      :>> #(format "(%s %s %s and %s)" (attr-str (nth e 1)) % (val-str (nth e 2)) (val-str (nth e 3)))
    '#{in} (cl-format nil "~a in(~{~a~^, ~})"
             (attr-str (nth e 1)) (map val-str (nth e 2)))
    ))

(defn- where-str
  [q] (expr-str q))

(defn select-str
  "Produces a string representing the query map in the SDB Select language.
  query calls this for you, just public for diagnostic purposes."
  [m]
  (str "select "
    (condp = (simplify-sym (:select m))
      '* "*"
      'ids "itemName()"
      'count "count(*)"
      (cl-format nil "~{~a~^, ~}" (map attr-str (:select m))))
    " from " (:from m)
    (when-let [w (:where m)]
      (str " where " (where-str w)))
    (when-let [s (:order-by m)]
      (str " order by " (attr-str (first s)) " " (or (second s) 'asc)))
    (when-let [n (:limit m)]
      (str " limit " n))))

(defn query
  "Issue a query. q is a map with mandatory keys:

  :select */ids/count/[sequence-of-attrs]
  :from domain-name

  and optional keys:

  :where sexpr-based query expr supporting

    (not expr)
    (and/or/intersection expr expr)
    (=/!=/</<=/>/>=/like/not-like attr val)
    (null/not-null attr)
    (between attr val1 val2)
    (in attr #(val-set})

  :order-by [attr] or [attr asc/desc]
  :limit n

  When :select is
      count - returns a number
      ids - returns a sequence of ids
      * or [sequence-of-attrs] - returns a sequence of item maps, containing all or specified attrs.

  See:

      http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/

  for further details of select semantics. Note query maps to the SDB Select, not Query, API
  next-token, if supplied, must be the value obtained from the :next-token attr of the metadata
  of a previous call to the same query, e.g. (:next-token (meta last-result))"
  ([client q] (query client q nil))
  ([client q next-token]
    (let [response (.select client (SelectRequest. (select-str q) next-token))
          response-meta (.getResponseMetadata response)
          result (.getSelectResult response)
          items (.getItem result)
          m {:box-usage (read-string (.getBoxUsage response-meta))
             :request-id (.getRequestId response-meta)
             :next-token (.getNextToken result)}]
      (condp = (simplify-sym (:select q))
        'count (-> items first .getAttribute (.get 0) .getValue Integer/valueOf)
        'ids (with-meta (map #(from-sdb-str (.getName %)) items) m)
        (with-meta (map (fn [item]
                          (build-item (from-sdb-str (.getName item)) (.getAttribute item)))
                     items)
          m)))))

(defn query-all
  "Issue a query repeatedly to get all results"
  [client q]
  (loop [ret [] next-token nil]
    (let [r1 (query client q next-token)
          nt (:next-token (meta r1))]
      (if nt
        (recur (into ret r1) nt)
        (with-meta (into ret r1) (meta r1)))))))
