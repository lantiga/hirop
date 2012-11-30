(ns hirop.core
  (:use [clojure.set :only [union select difference]])
  (:require [clojure.string :as string]))

(defn- #^String chop
  "Removes the last character of string, does nothing on a zero-length
  string. From clojure.contrib."
  [#^String s]
  (let [size (count s)]
    (if (zero? size)
      s
      (subs s 0 (dec (count s))))))

;; https://gist.github.com/1302024
#_(defn- md5
  "Generate a md5 checksum for the given string"
  [token]
  (let [hash-bytes
         (doto (java.security.MessageDigest/getInstance "MD5")
               (.reset)
               (.update (.getBytes token)))]
       (.toString
         (new java.math.BigInteger 1 (.digest hash-bytes)) ; Positive and the size of the number
         16))) ; Use base16 i.e. hex

(defn- relation-field?
  [field]
  (= \_ (last (name field))))

(defn- doctype->relation
  [doctype]
  (if (not= \_ (last (name doctype)))
    (keyword (str (name doctype) "_"))
    doctype))

(defn- relation->doctype
  [relation]
  (if (= \_ (last (name relation)))
    (keyword (chop (name relation)))
    relation))

(defn- get-relation-fields
  [context doctype val]
  (reduce
   (fn [res el]
     (if (or
          (= doctype (:from el))
          (= :* (:from el)))
       (assoc res (doctype->relation (:to el)) val)
       res))
   {}
   (:relations context)))

(defn get-external-doctypes
  [context]
  (map :to (filter :external (:relations context))))

(defn get-free-external-doctypes
  [context]
  (filter #(not (contains? (:external-ids context) %)) (get-external-doctypes context)))

;; TODO: here we should check that all relations marked as external are specified in external-ids, otherwise fail
(defn init-context
  [context-name context doctypes configurations external-ids]
  (->
   context
   (assoc :name context-name)
   (assoc :doctypes doctypes)
   (assoc :configurations configurations)
   (assoc :external-ids external-ids)))

(defn get-doctype
  [context doctype]
  (let [configuration (-> context :configurations doctype)
        relations (get-relation-fields context doctype "String")]
    (->
     (-> context :doctypes doctype)
     (assoc
         :_id {}
         :_rev {}
         :_type {}
         :_meta {}
         :_conf configuration)
     (merge relations))))

(def empty-document
     {:_id nil
      :_rev nil
      :_meta nil
      :_type nil
      :_conf nil})

;; TODO: here we embed configuration into each document.
;;      :_conf (md5 (json/generate-string configuration))))))
(defn new-document
  [context doctype]
  (let [fields (zipmap (keys (-> context :doctypes doctype :fields)) (repeat ""))
        configuration (-> context :configurations doctype)
        relations (get-relation-fields context doctype nil)
        relations (reduce
                   (fn [rels [k v]] (let [k (doctype->relation k)] (if (contains? rels k) (assoc rels k v) rels)))
                   relations
                   (-> context :external-ids))]
    (->
     empty-document
     (assoc
         :_type (name doctype)
         :_conf configuration)
     (merge fields)
     (merge relations))))

(defn new-store
  ([context-name]
     (new-store context-name {}))
  ([context-name meta]
     {:uuid 0
      :context-name context-name
      :push-result nil
      :merge-result nil
      :meta meta
      :stored {}
      :starred {}
      :baseline {}
      :configurations {}
      :remote #{}
      :local #{}
      :selections {}}))

;; TODO: for serializing, (assoc store (zipmap (:remote store) (repeat nil)))

(defn inc-uuid
  [store]
  (update-in store [:uuid] inc))

(defn get-uuid
  [store]
  (str "tmp" (:uuid store) ""))

(defn- rev-number [rev]
  (if (string? rev)
    (if (re-find #"-" rev)
      ;*CLJSBUILD-REMOVE*;(cljs.reader/read-string (first (string/split rev #"-")))
      ;*CLJSBUILD-REMOVE*;#_
      (read-string (first (string/split rev #"-")))
      ;*CLJSBUILD-REMOVE*;(cljs.reader/read-string rev)
      ;*CLJSBUILD-REMOVE*;#_
      (read-string rev))
    rev))

(defn- compare-revs
  [rev1 rev2]
  (compare (rev-number rev1) (rev-number rev2)))

(defn older?
  [document1 document2]
  (neg? (compare-revs (:_rev document1) (:_rev document2))))

(def newer?
     (complement older?))

(defn- get-id
  [doc-or-id]
  (if (string? doc-or-id)
    doc-or-id
    (:_id doc-or-id)))

(defn get-document
  ([store id]
     (let [starred (get-in store [:starred id])
           stored (get-in store [:stored id])]
       (cond
        starred starred
        :else stored)))
  ([store id bucket]
     (get-in store [bucket id])))

(defn get-stored
  [store id]
  (get-in store [:stored id]))

(defn get-baseline
  [store id]
  (let [baseline (get-in store [:baseline id])
        stored (get-in store [:stored id])]
    (cond
     baseline baseline
     :else stored)))

(defn document-state
  [store id]
  (let [starred (get-in store [:starred id])
        stored (get-in store [:stored id])]
    (cond
     starred :starred
     :else :stored)))

(defn starred?
  [store id]
  (= :starred (document-state store id)))

(defn stored?
  [store id]
  (= :stored (document-state store id)))

(defn revision
  ([store id]
     (:_rev ((get-document store id))))
  ([store id bucket]
     (:_rev ((get-document store id bucket)))))

(defn add-document
  ([store document]
     (add-document store document :stored))
  ([store document bucket]
     (assoc-in store [bucket (:_id document)] document)))

(defn add-document-if-newer
  ([store document]
     (add-document-if-newer store document :stored))
  ([store document bucket]
     (if (newer? document (get-document store (:_id document)))
       (add-document store document bucket)
       store)))

(defn add-to-set
  [store document set-name]
  (update-in store [set-name] #(conj % (:_id document))))

(defn add-to-local
  [store document]
  (add-to-set store document :local))

(defn add-to-remote
  [store document]
  (add-to-set store document :remote))

(defn zero-rev
  []
  nil)

(defn commit
  [store document]
  ;; In a multi-threaded environment, this should be atomic (like all the others, in any case)
  ;; Here baseline could already be at a greater revision number compared to the committed document
  ;; had the latter been checked out some time before the commit.
  (let [baseline (get-stored store (:_id document))
        timestamp (.. (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ssZ") (format (java.util.Date.)))
        document (assoc document :_meta (assoc (:meta store) :timestamp timestamp))
        store (if (:_id document) store (inc-uuid store))
        document (if (:_id document) document (assoc document :_id (get-uuid store) :_rev (zero-rev)))]
    (-> store
        ((fn [store] (if baseline (add-document store baseline :baseline) store)))
        (add-document document :starred)
        (add-to-local document))))

(defn mcommit
  [store documents]
  (reduce (fn [store document] (commit store document)) store documents))

(defn fetch
  [store context fetcher]
  (let [context-name (:name context)
        external-ids (:external-ids context)
        boundaries (get-free-external-doctypes context)
        documents (fetcher context-name external-ids boundaries)]
    (reduce
     (fn [store document]
       (-> store
           (add-document-if-newer document :stored)
           (add-to-remote document)))
     store
     documents)))

(defn conflicted?
  [store id]
  (let [starred (dissoc (get-document store id :starred) :_rev :_meta)
        stored (dissoc (get-document store id :stored) :_rev :_meta)
        baseline (dissoc (get-document store id :baseline) :_rev :_meta)]
    (if (nil? starred)
      false
      (and
       (not= starred baseline)
       (not= stored baseline)))))

(defn get-conflicted-ids
  [store]
  (filter (partial conflicted? store) (:local store)))

(defn any-conflicted?
  [store]
  (not (empty? (get-conflicted-ids store))))

(defn checkout-conflicted
  [store]
  (group-by
   #(get-in % [:stored :_type])
   (map
    (fn [id]
      {:stored (get-document store id :stored)
       :starred (get-document store id :starred)
       :baseline (get-document store id :baseline)})
   (get-conflicted-ids store))))

(defn merge-document
  ;; In case of fast-forward, we advance :_rev, otherwise we don't.
  ;; TODO: _meta should be only overwritten and shouldn't originate a conflict
  [store id]
  (let [starred (get-document store id :starred)
        stored (get-document store id :stored)
        baseline (get-document store id :baseline)]
    (if starred
      (let [merged
            (reduce
             (fn [document key]
               (assoc document key (get stored key)))
             starred
             (filter
              (fn [key]
                (and
                 (not (contains? #{:_id :_rev} key))
                 (= (get starred key) (get baseline key))))
              (keys stored)))
            merged (if (conflicted? store id) merged (assoc merged :_rev (get stored :_rev)))]
        (add-document store merged :starred))
      store)))

(defn merge-remote
  ;; for every remote, merge into local
  ;; since local and remote share :stored, this effectively means:
  ;; for every remote that is starred, merge unconflicted fields from :stored to :starred
  ;; unconflicted means those fields in stored that are unchanged between baseline and starred
  ;; keep in local only those that are either starred (or locked?) and those that come from remote
  [store]
  (-> store
      ((fn [store]
        (reduce merge-document store (select (partial starred? store) (:remote store)))))
      ((fn [store]
        (update-in store [:local] #(union (select (partial starred? store) %) (:remote store)))))))

(defn pull
  [store context fetcher]
  (-> store (fetch context fetcher) merge-remote))

(defn- unstar
  ;; move starred to stored (really?), remove baselines (locked?)
  ([store]
    (unstar store (keys (:starred store))))
  ([store starred-ids]
     (-> store
         (update-in [:stored] #(merge % (select-keys (:starred store) starred-ids)))
         ;; this only removes the starred and baselines relative to the provided starred
         (update-in [:starred] #(apply dissoc % starred-ids))
         (assoc [:baseline] #(apply dissoc % starred-ids)))))

(defn- set-merge-result
  [store result]
  (assoc store :merge-result result))

(defn- set-push-result
  [store result]
  (assoc store :push-result result))

(defn get-push-result
  [store]
  (:push-result store))

(defn- remap-tmp-ids
  [store tmp-map]
  (->
   store
   (update-in
    [:starred]
    (fn [starred]
      (into
       {}
       (map
        (fn [[id doc]]
          (let [new-id (or (get tmp-map id) id)
                new-relations
                (into {} (map
                          (fn [[k val]]
                            (if (coll? val)
                              [k (vec (map #(or (get tmp-map %) %) val))]
                              [k (or (get tmp-map val) val)]))
                          (select-keys doc (filter relation-field? (keys doc)))))
                new-doc (-> doc (assoc :_id new-id) (merge new-relations))]
            [new-id new-doc]))
        starred))))
   (update-in
    [:local]
    (fn [local]
      (->
       local
       (difference (set (keys tmp-map)))
       (union (set (vals tmp-map))))))
   (assoc :selections
     (into
      {}
      (map
       (fn [[sel-id sel]]
         (let [remapped-sel
               (into
                {}
                (map
                 (fn [[doctype doc-ids]]
                   (let [doc-ids-coll (if (coll? doc-ids) doc-ids [doc-ids])
                         remapped-doc-ids
                         (map
                          (fn [doc-id]
                            (if (contains? tmp-map doc-id)
                              (tmp-map doc-id)
                              doc-id))
                          doc-ids-coll)
                         remapped-doc-ids (if (coll? doc-ids) remapped-doc-ids (first remapped-doc-ids))]
                     [doctype remapped-doc-ids]))
                 sel))]
           [sel-id remapped-sel]))
       (:selections store))))))

(defn push-save
  [store saver]
  (let [starred (:starred store)]
    {:save-ret (saver (vals starred) (:context-name store))
     :starred starred}))

(defn push-post-save
  ;; this is decoupled from push to provide a side-effect free option
  ;; Also, save-info contains starred, which typically are the starred 
  ;; that were considered in the save.
  [store save-info]
  (let [store (if (= :success (get-in save-info [:save-ret :result]))
                (-> store
                    (remap-tmp-ids (get-in save-info [:save-ret :remap]))
                    (unstar (map #(get (get-in save-info [:save-ret :remap]) % %) (keys (:starred save-info)))))
                store)]
    (set-push-result store (get-in save-info [:save-ret :result]))))

(defn push
  ;; save, upon success unstar, otherwise return store unmodified
  ;; leave it to a higher order function to reiterate
  [store saver]
  (let [ret (push-save store saver)]
    (push-post-save store ret)))

(defn- get-ids-of-type
  [store doctype]
  (select #(= (name doctype) (name (:_type (get-document store %)))) (:local store)))

(defn checkout
  ([store]
     (map #(get-document store %) (:local store)))
  ([store doctype]
     (map #(get-document store %) (get-ids-of-type store doctype))))

(defn get-selected-ids
  ([store selection-id]
     (get-in store [:selections selection-id]))
  ([store selection-id doctype]
     (get-in store [:selections selection-id doctype])))

(defn checkout-selected
  ([store selection-id doctype]
     (let [ids (get-selected-ids store selection-id doctype)]
       (if (string? ids)
         (get-document store ids)
         (map #(get-document store %) ids))))
  ([store selection-id]
     (into {}
           (map
            (fn [[doctype _]] [doctype (checkout-selected store selection-id doctype)])
            (get-selected-ids store selection-id)))))

;; TODO: upon loading the context, create the relations graph for efficiency
(defn- get-relations
  [context doctype direction]
  (condp = direction
      :out (filter (fn [rel] (= doctype (rel :from))) (context :relations))
      :in (filter (fn [rel] (= doctype (rel :to))) (context :relations))))

(defn- set-relation-ids
  [store context selection-id doctype rel-doctype field rel-field]
  (let [ids (get-in store [:selections selection-id doctype])]
    (reduce
     (fn [store id]
       (let [values (get (get-document store id) field)
             values (if (coll? values) values [values])
             rel-ids
             (reduce
              (fn [out value]
                (let [rel-ids
                      (filter
                       (fn [rel-id]
                         (let [rel-value (get (get-document store rel-id) rel-field)]
                           (if (coll? rel-value)
                             (some #(= value %) rel-value)
                             (= value rel-value))))
                       (get-ids-of-type store rel-doctype))
                      rel-ids (if-let [sort-keys (get-in context [:selections selection-id rel-doctype :sort-by])]
                                (sort-by (fn [el] ((apply juxt sort-keys) (get-document store el))) rel-ids)
                                rel-ids)
                      rel-ids (condp = (get-in context [:selections selection-id rel-doctype :select])
                                :first (if (first rel-ids) [(first rel-ids)] [])
                                :last (if (last rel-ids) [(last rel-ids)] [])
                                :all rel-ids
                                rel-ids)]
                  (concat out rel-ids)))
              []
              values)]
         (if (some empty? [values rel-ids])
           store
           (update-in store [:selections selection-id rel-doctype] #(vec (distinct (concat % rel-ids)))))))
     store
     ids)))

(defn- propagate-selection
  ;; recursively propagate selection backward and forward, starting from id and making sure a document
  ;; type is not selected twice, return store with updated selection
  ;; TODO: we have hooks in the Javascript version that are called everytime a selection changes.
  ;; Here we could keep track of who changed at the end of the propagation (better, called once)
  [store context selection-id doctypes]
  (loop [store store
         doctypes doctypes
         visited (into #{} doctypes)]
    (if (empty? doctypes)
      store
      (let [doctype (first doctypes)
            out (filter #(not (contains? visited (:to %))) (get-relations context doctype :out))
            in (filter #(not (contains? visited (:from %))) (get-relations context doctype :in))]
        (recur
         (->
          store
          ((fn [store]
             (reduce (fn [store rel] (set-relation-ids store context selection-id doctype (:to rel) (doctype->relation (:to rel)) :_id)) store out)))
          ((fn [store]
             (reduce (fn [store rel] (set-relation-ids store context selection-id doctype (:from rel) :_id (doctype->relation (:to rel)))) store in))))
         (concat (rest doctypes) (map :to out) (map :from in))
         (conj visited doctype))))))

;; TODO: support multiple selection
(defn select-document
  [store context id selection-id]
  (let [doctype (keyword (:_type (get-document store id)))]
    (-> store
        (assoc-in [:selections selection-id] nil)
        (assoc-in [:selections selection-id doctype] [id])
        (propagate-selection context selection-id [doctype]))))

(defn unselect
  [store context selection-id doctype]
  (-> store
      (assoc-in [:selections selection-id doctype] nil)
      (propagate-selection context selection-id [doctype])))

;; TODO: support multiple external-ids of the same type
(defn select-defaults
  [store context selection-id]
  (let [store (assoc-in store [:selections selection-id] nil)
        store
        (reduce
         (fn [store [external-doctype external-id]]
           (assoc-in store [:selections selection-id external-doctype] [external-id]))
         store
         (:external-ids context))]
    (propagate-selection store context selection-id (map first (:external-ids context)))))

