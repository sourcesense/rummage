;   Copyright (c) Chas Emerick, Rich Hickey, and other contributors. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns cemerick.rummage
  (:require [cemerick.rummage.encoding :as encoding])
  (:import
    cemerick.rummage.DataUtils
    (com.amazonaws.services.simpledb AmazonSimpleDBClient)
    (com.amazonaws.services.simpledb.model CreateDomainRequest DeleteDomainRequest
      ListDomainsRequest DomainMetadataRequest Attribute
      ReplaceableItem ReplaceableAttribute PutAttributesRequest
      GetAttributesRequest UpdateCondition BatchPutAttributesRequest)))

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
  [client name]
  (.createDomain ^AmazonSimpleDBClient (or (:client client) client)
    (CreateDomainRequest. name)))

(defn delete-domain
  "Deletes the named domain."
  [client name]
  (.deleteDomain ^AmazonSimpleDBClient (or (:client client) client)
    (DeleteDomainRequest. name)))

(defn- list-domains*
  [client next-token]
  (let [req (-> (ListDomainsRequest.) (.withNextToken next-token))
        res (.listDomains ^AmazonSimpleDBClient (or (:client client) client) req)]
    (concat (.getDomainNames res)
      (when (.getNextToken res)
        (list-domains* client (.getNextToken res))))))

(defn list-domains
  "Returns a sequence of all domain names available from the given client."
  [client]
  (list-domains* client nil))

(def domain-metadata-keys
  #{:timestamp :attributeValuesSizeBytes :attributeNameCount :itemCount
    :attributeValueCount :attributeNamesSizeBytes :itemNamesSizeBytes})

(defn domain-metadata 
  "Returns a map of domain metadata"
  [client domain]
  (select-keys
    (-> ^AmazonSimpleDBClient (or (:client client) client)
      (.domainMetadata (DomainMetadataRequest. domain))
      bean)
    domain-metadata-keys))

(defn- as-collection
  "If v is a collection, returns it, else returns a collection containing v."
  [v]
  (if (or (coll? v) (instance? java.util.Collection v)) v [v]))

(defn- as-set
  "If v is a set, returns it, else returns a set containing v."
  [v]
  (if (set? v) v (hash-set v)))

(defn- build-attrs
  [encode-fn item add-to?]
  (for [[k v] item
        v (as-collection v)
        :let [[name value] (encode-fn [k v])]]
    (ReplaceableAttribute. name value (if add-to?
                                        (not (add-to? k))
                                        true))))

(defn- update-condition
  [encode-fn [key value :as expectation-pair] exists?]
  (let [[k v] (encode-fn expectation-pair)]
    (UpdateCondition. k (and value v) exists?)))

(defn put-attrs
  "Puts attrs for one item into the domain. By default, attrs replace
  all values present at the same attrs/keys. You can pass an add-to?
  function (usually a set), and when it returns true for a key, values
  will be added to the set of values at that key, if any."
  [client-config domain item & {:keys [add-to? expecting not-expecting]}]
  (when (and expecting not-expecting)
    (throw (IllegalArgumentException. "Cannot have both :expecting and :not-expecting update conditions")))
  (let [id ((:encode-id client-config) (:sdb/id item))
        attrs (build-attrs (:encode client-config) item add-to?)
        update-condition (cond
                           expecting (update-condition (:encode client-config) (as-collection expecting) true)
                           not-expecting (update-condition (:encode client-config) [not-expecting] false))]
    (.putAttributes
      ^AmazonSimpleDBClient (or (:client client-config) client-config)
      (.withExpected (PutAttributesRequest. domain id attrs) update-condition))))

(defn- build-items
  [schema items add-to?]
  (for [i items]
    (ReplaceableItem. ((:encode-id schema) (:sdb/id i))
      (build-attrs (:encode schema) i add-to?))))

(defn put-all-attrs
  "Puts the attrs for multiple items into a domain, with the same semantics as put-attrs"
  [client-config domain items & {:keys [add-to?]}]
  (doseq [batch (partition-all 25 (build-items client-config items add-to?))]
    (.batchPutAttributes
      ^AmazonSimpleDBClient (or (:client client-config) client-config)
      (BatchPutAttributesRequest. domain batch))))

(defn- into-map
  [decode-fn item-id attributes]
  (when-not decode-fn
    (throw (IllegalArgumentException. "No :decode function available in client-config")))
  (reduce
    (fn [m ^Attribute a]
      (let [[k v] (decode-fn [(.getName a) (.getValue a)])]
        (update-in m [k] #(if % (-> % as-set (conj %2)) %2) v)))
    {:sdb/id item-id}
    attributes))

(defn get-attrs
  [client-config domain item-id & attr-names]
  (let [req (-> (GetAttributesRequest. domain ((:encode-id client-config) item-id))
              (.withConsistentRead (-> client-config :consistent-read? boolean))
              (.withAttributeNames (->> attr-names
                                     (map #((:encode client-config) [% nil]))
                                     (map first))))
        res (.getAttributes ^AmazonSimpleDBClient (or (:client client-config) client-config) req)]
    (when-let [attrs (-> res .getAttributes seq)]
      (into-map (:decode client-config) item-id attrs))))

#_(comment

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
