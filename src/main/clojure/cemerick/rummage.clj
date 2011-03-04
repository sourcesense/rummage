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
      GetAttributesRequest UpdateCondition BatchPutAttributesRequest
      DeleteAttributesRequest DeletableItem BatchDeleteAttributesRequest
      SelectRequest Item)))

(defn create-client
  "Creates a client for talking to a specific AWS SimpleDB
  account. The same client can be reused for multiple requests."
  ([id secret-key]
    (create-client id secret-key (com.amazonaws.ClientConfiguration.)))
  ([id secret-key client-config]
    (AmazonSimpleDBClient. (com.amazonaws.auth.BasicAWSCredentials. id secret-key)
      (.withUserAgent client-config "Rummage - SDB for Clojure"))))

(defn- ^AmazonSimpleDBClient client
  [client-or-config]
  (or (:client client-or-config) client-or-config))

(defn create-domain
  "Creates a domain with the specified name.  Returns successfully if the domain
   already exists."
  [client-or-config name]
  (.createDomain (client client-or-config) (CreateDomainRequest. name)))

(defn delete-domain
  "Deletes the named domain."
  [client-or-config name]
  (.deleteDomain (client client-or-config)
    (DeleteDomainRequest. name)))

(defn- list-domains*
  [client-or-config next-token]
  (let [req (-> (ListDomainsRequest.) (.withNextToken next-token))
        res (.listDomains (client client-or-config) req)]
    (concat (.getDomainNames res)
      (when (.getNextToken res)
        (list-domains* client (.getNextToken res))))))

(defn list-domains
  "Returns a sequence of all domain names available from the given client."
  [client-or-config]
  (list-domains* (client client-or-config) nil))

(def domain-metadata-keys
  #{:timestamp :attributeValuesSizeBytes :attributeNameCount :itemCount
    :attributeValueCount :attributeNamesSizeBytes :itemNamesSizeBytes})

(defn domain-metadata 
  "Returns a map of domain metadata"
  [client-or-config domain]
  (select-keys
    (-> (client client-or-config)
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
  (for [[k v] (dissoc item :sdb/id)
        v (as-collection v)
        :let [[name value] (encode-fn [k v])]]
    (ReplaceableAttribute. name value (if add-to?
                                        (not (add-to? k))
                                        true))))

(defn- update-condition
  [encode-fn [key value :as expecting] not-expecting]
  (when (and expecting not-expecting)
    (throw (IllegalArgumentException. "Cannot have both :expecting and :not-expecting update conditions")))
  (cond
    expecting (let [[k v] (-> expecting as-collection encode-fn)]
                (UpdateCondition. k (and value v) true))
    not-expecting (let [[k v] (encode-fn [not-expecting])]
                    (UpdateCondition. k nil false))))

(defn put-attrs
  "Puts attrs for one item into the domain. By default, attrs replace
  all values present at the same attrs/keys. You can pass an add-to?
  function (usually a set), and when it returns true for a key, values
  will be added to the set of values at that key, if any."
  [client-config domain item & {:keys [add-to? expecting not-expecting]}]
  (let [id ((:encode-id client-config) (:sdb/id item))
        attrs (build-attrs (:encode client-config) item add-to?)
        update-condition (update-condition (:encode client-config) expecting not-expecting)]
    (.putAttributes
      (client client-config)
      (.withExpected (PutAttributesRequest. domain id attrs) update-condition))))

(defn- encode-item
  [{:keys [encode encode-id]} add-to? item]
  (ReplaceableItem. (encode-id (:sdb/id item))
    (build-attrs encode item add-to?)))

(defn batch-put-attrs
  "Puts the attrs for multiple items into a domain, with the same semantics as put-attrs"
  [client-config domain items & {:keys [add-to?]}]
  (doseq [batch (partition-all 25 (map (partial encode-item client-config add-to?) items))]
    (.batchPutAttributes
      (client client-config)
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
        res (.getAttributes (client client-config) req)]
    (when-let [attrs (-> res .getAttributes seq)]
      (into-map (:decode client-config) item-id attrs))))

(defn- build-delete-attrs
  [encode-fn attrs]
  (let [attrs (cond
                (or (empty? attrs) (map? attrs)) attrs
                (set? attrs) (for [key attrs] [key nil])
                :else (throw (IllegalArgumentException.
                               ":attrs must be a set of attribute keys, a map of attribute keys and values, or absent to delete all attributes of item-id")))]
    (for [[key value :as pair] attrs
          :let [[name avalue] (encode-fn pair)]]
      (if value
        (Attribute. name avalue)
        (.withName (Attribute.) name)))))

(defn delete-attrs
  "Deletes the attrs from the item. If no attrs are supplied, deletes
  all attrs and the item. attrs can be a set, in which case all values
  at those keys will be deleted, or a map, in which case only the
  values supplied will be deleted."
  [client-config domain item-id & {:keys [attrs expecting not-expecting]}]
  (when (and expecting not-expecting)
    (throw (IllegalArgumentException. "Cannot have both :expecting and :not-expecting delete conditions")))
  (let [encode-fn (:encode client-config)
        attrs (build-delete-attrs encode-fn attrs)
        update-condition (update-condition encode-fn expecting not-expecting)]
    (.deleteAttributes
      (client client-config)
      (DeleteAttributesRequest. domain ((:encode-id client-config) item-id) attrs update-condition))))

(defn batch-delete-attrs
  [client-config domain item-attrs]
  (let [encode-id (:encode-id client-config)
        encode-fn (:encode client-config)
        del-items (for [[item-id attrs] item-attrs]
                    (DeletableItem. (encode-id item-id) (build-delete-attrs encode-fn attrs)))]
    (doseq [batch (partition-all 25 del-items)]
      (.batchDeleteAttributes
        (client client-config)
        (BatchDeleteAttributesRequest. domain batch)))))

(def ^{:private true} *select-encode-fn*)
(def ^{:private true} *select-encode-id-fn*)

(defn- escape
  ([name] (str \` name \`))
  ([name ^String value]
    [(escape name)
     (str \" (.replace value "\"" "\"\"") \")]))

(defn- strip-expr-symbol-ns
  [expr]
  (-> expr first name symbol))

(defn- handle-every
  ([attr]
  (if-not (sequential? attr)
    attr
    (do
      (when (not= 'every (strip-expr-symbol-ns attr))
        (throw (IllegalArgumentException. (str "Invalid query op: " attr))))
      (format "every(%s)" (*select-encode-fn* (second attr))))))
  ([attr value] [(handle-every attr) value]))

(defn- where-str
  [where-expansions expr]
  (let [expr-op (strip-expr-symbol-ns expr)
        expansion-fn (where-expansions expr-op)]
    (when-not expansion-fn
      (throw (IllegalArgumentException. "No expansion available for where expression " expr)))
    (apply expansion-fn (-> expr-op name (.replace "-" " ")) (rest expr))))

(def ^{:private true} where-expansions
  (let [base {'not #(format "(not %s)" (where-str %2))
              
              '[and or intersection]
              #(apply format "(%2$s %1$s %3$s)" % (map where-str %&))
              
              '[= != < <= > >= like not-like]
              (fn [op & name-value]
                (apply format "(%2$s %1$s %3$s)" op (apply *select-encode-fn* name-value)))
              
              '[null not-null]
              #(format "(%s is %s)" (*select-encode-fn* %2) %)
              
              'between
              (fn [_ name val1 val2]
                (let [[name val1] (*select-encode-fn* name val1)
                      [_ val2] (*select-encode-fn* name val2)]
                  (format "(%s between %s and %s)" name val1 val2)))
              
              'in
              (fn [_ name values]
                ; the repeated encoding of `name` here given large sets of values is unfortunate
                (let [pairs (map #(*select-encode-fn* name %) values)]
                  (format "%s in(%s)" (ffirst pairs) (->> pairs
                                                       (map second)
                                                       (interpose ", ")
                                                       (apply str)))))}]
    (->> base
      (mapcat (fn [[k v]]
                (if (coll? k)
                  (for [op k] [op v]))))
      (into {}))))

(def ^{:private true} query-language
  {:select #(case %
              '* "*"
              'id "itemName()"
              'count "count(*)"
              (interpose ", " (map *select-encode-fn* %)))
   :from escape
   :where (partial where-str where-expansions)
   :order-by (fn [[attr] & [direction]]
               [(*select-encode-fn* attr) \space (or direction "asc")])
   :limit identity})

(defn select-string
  "Produces a string representing the query map in the SDB Select language.
  `query` calls this for you – public only for diagnostic purposes."
  [config select-map]
  (binding [*select-encode-fn* (comp
                                 (partial apply escape)
                                 (partial apply (:encode config))
                                 handle-every)
            *select-encode-id-fn* (comp escape (:encode-id config))]
    (->> (for [k [:select :from :where :order-by :limit]
               :let [expansion-fn (k query-language)
                     expression (k select-map)]
               :when expression]
           (cons [\space (-> k name (.replace "-" " ")) \space]
             (-> expression expansion-fn as-collection)))
      (mapcat identity)
      (apply str))))

(defn- decode-item
  [{:keys [decode-id decode]} ^Item item]
  (assoc (->> (.getAttributes item)
           (map (fn [^Attribute a]
                  (decode (.getName a) (.getValue a))))
           (into {}))
    :sdb/id (-> item .getName decode-id)))

(defn query
  "Issue a query. When `q` is a string, it is submitted directly without any interpretation.

   When `q` is a map, it is interpreted to generate a corresponding query string,
   using the configuration provided to drive attribute name and value formatting.

   The query map has mandatory keys:

   :select */id/count/[sequence-of-attrs]
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
      id - returns a sequence of ids
      * or [sequence-of-attrs] - returns a sequence of item maps, containing all or specified attrs.

   See:

      http://docs.amazonwebservices.com/AmazonSimpleDB/latest/DeveloperGuide/index.html?UsingSelect.html

  for further details of select semantics.

  If the `client-config` map contains a truthy :consistent-read? value, then the query will
  be performed in SDB's consistent read mode.

  `next-token`, if supplied, must be the value obtained from the :next-token attr of the metadata
  of a previous call to the same query, e.g. (:next-token (meta last-result)). When provided,
  the next batch of results is returned.  See `query-all` to get all results in a single lazy seq."
  ([client-config q] (query client-config q nil))
  ([client-config q next-token]
    (let [query (if (string? q)
                  q
                  (->> q
                    (map (fn [[k v]] [(-> k name keyword) v]))
                    (into {})
                    (select-string client-config)))
          request (-> query
                    (SelectRequest. (-> client-config :consistent-read? boolean))
                    (.withNextToken next-token)) 
          response (.select (client client-config) request)
          items (.getItems response)
          result (case (-> #"^\s*select\s+(count\(\*\)|itemName\(\))" (re-seq query) second)
                   "count(*)" (-> items first .getAttributes first .getValue Long/valueOf)
                   "itemName()" (map #((:decode-id client-config) (.getName ^Item %)) items)
                   (map (partial decode-item client-config) items))]
      (if-not (coll? result)
        result
        (let [response-meta (.getCachedResponseMetadata (client client-config) request)]
          (with-meta result {:box-usage (.getBoxUsage response-meta) 
                             :request-id (.getRequestId response-meta)
                             :next-token (.getNextToken response)}))))))

(defn- query-all*
  [client q next-token]
  (let [res (query client q next-token)]
    (if-let [nt (-> res meta :next-token)]
      (concat res
        (lazy-seq (query-all* client q next-token)))
      res)))

(defn query-all
  "Returns a lazy seq of all results of the given query.  See `query` for details."
  [client q]
  (query-all client q nil))
