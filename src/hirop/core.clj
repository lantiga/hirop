(ns hirop.core
  (:use [clojure.set :only [union select difference]])
  (:require [clojure.string :as string]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO: implement prototypes
#_{:prototypes
   {:Contents [:ContentA :ContentB]
    :Taggable [:Event :Contents]}
   ;; :cardinality :one :cardinality :many
   
   :relations
   [{:from :Event :to :Patient :external true :cardinality :one}
    {:from :Contents :to :Event}]
   
   :selections
   {:default
    {:Patient {:sort-by [:id] :default :last}
     :Contents {:sort-by [:title] :default :all}}}}
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; The implementation of prototypes could rely on
;; expanding prototypes at the create-context level
;; and reusing existing code immodified (except
;; for querying a prototype -> doctype map when
;; querying for a prototype

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

(defn hid [doc]
  (get-in doc [:_hirop :id]))

(defn hrev [doc]
  (get-in doc [:_hirop :rev]))
;;  (or (get-in doc [:_hirop :rev]) 0))

(defn hrels [doc]
  (get-in doc [:_hirop :rels]))

(defn hrel [doc doctype]
  (get-in doc [:_hirop :rels doctype]))

(defn htype [doc]
  (keyword (get-in doc [:_hirop :type])))

(defn hmeta [doc]
  (get-in doc [:_hirop :meta]))

(defn hconf [doc]
  (get-in doc [:_hirop :conf]))

(defn assoc-hid [doc id]
  (assoc-in doc [:_hirop :id] id))

(defn assoc-hrev [doc rev]
  (assoc-in doc [:_hirop :rev] rev))

(defn assoc-hrels [doc rels]
  (assoc-in doc [:_hirop :rels] rels))

(defn assoc-hrel [doc doctype rel-id]
  (assoc-in doc [:_hirop :rels doctype] rel-id))

(defn assoc-htype [doc type]
  (assoc-in doc [:_hirop :type] (name type)))

(defn assoc-hmeta [doc meta]
  (assoc-in doc [:_hirop :meta] meta))

(defn assoc-hconf [doc conf]
  (assoc-in doc [:_hirop :conf] conf))

(defn is-temporary-id?
  [id]
  (string? (re-find #"^tmp" id)))

(defn has-temporary-id?
  [doc]
  (is-temporary-id? (hid doc)))

(defn- get-relation-fields
  ;; TODO: prototypes
  [context doctype val]
  (reduce
   (fn [res el]
     (if (or
          (= doctype (:from el))
          (= :* (:from el)))
       (assoc res (:to el) val)
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
(defn create-context
  [context-name context doctypes configurations external-ids]
  ;; TODO: prototypes
  (->
   context
   (assoc :name context-name)
   (assoc :doctypes doctypes)
   (assoc :configurations configurations)
   (assoc :external-ids external-ids)))

(defn get-doctype
  [context doctype]
  (let [configuration (get-in context [:configurations doctype])
        relations (get-relation-fields context doctype "String")]
    (->
     (get-in context [:doctypes doctype])
     (assoc :_hirop
       {:id {}
        :rev {}
        :type {}
        :meta {}
        :conf configuration
        :rels relations}))))

(def empty-document
  {:_hirop
   {:id nil
    :rev nil
    :meta nil
    :type nil
    :conf nil
    :rels nil}})

;; TODO: here we embed configuration into each document.
;;      :_conf (md5 (json/generate-string configuration))))))
(defn new-document
  [context doctype]
  (let [fields (zipmap (keys (get-in context [:doctypes doctype :fields])) (repeat ""))
        configuration (get-in context [:configurations doctype])
        relations (get-relation-fields context doctype nil)
        relations (reduce
                   (fn [rels [k v]]
                     (if (contains? rels k)
                       (assoc rels k v)
                       rels))
                   relations
                   (get context :external-ids))]
    (->
     empty-document
     (assoc-htype doctype)
     (assoc-in [:_hirop :conf] configuration)
     (assoc-hrels relations)
     (merge fields))))

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
      :revisions {}
      :selections {}}))

;; TODO: for serializing, (assoc store (zipmap (:remote store) (repeat nil)))

(defn inc-uuid
  [store]
  (update-in store [:uuid] inc))

(defn get-uuid
  [store]
  (str "tmp" (:uuid store) ""))

(defn- get-id
  [doc-or-id]
  (if (string? doc-or-id)
    doc-or-id
    (hid doc-or-id)))

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

(defn revisions
  [store id]
  (get-in store [:revisions id]))

(defn add-document
  ([store document]
     (add-document store document :stored))
  ([store document bucket]
     (->
      store
      (assoc-in [bucket (hid document)] document)
      (update-in [:revisions (hid document)] conj (hrev document)))))

(defn add-document-if-new
  ([store document]
     (add-document-if-new store document :stored))
  ([store document bucket]
     (if (contains? (set (revisions store (hid document))) (hrev document))
       store
       (add-document store document bucket))))

(defn add-to-set
  [store document set-name]
  (update-in store [set-name] #(conj % (hid document))))

(defn add-to-local
  [store document]
  (add-to-set store document :local))

(defn add-to-remote
  [store document]
  (add-to-set store document :remote))

(defn commit
  [store document]
  ;; In a multi-threaded environment, this should be atomic (like all the others, in any case)
  ;; Here baseline could already be at a greater revision number compared to the committed document
  ;; had the latter been checked out some time before the commit.
  (let [baseline (get-stored store (hid document))
        timestamp
        ;*CLJSBUILD-REMOVE*;(.toISOString (js/Date.))
        ;*CLJSBUILD-REMOVE*;#_
        (.. (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ssZ") (format (java.util.Date.)))
        document (assoc-hmeta document
                              (merge (hmeta document) (:meta store) {:timestamp timestamp}))
        store (if (hid document) store (inc-uuid store))
        document (if (hid document)
                   document
                   (assoc-hid document (get-uuid store)))
        store (if baseline (add-document store baseline :baseline) store)]
    (-> store
        (add-document document :starred)
        (add-to-local document))))

(defn mcommit
  [store documents]
  (reduce (fn [store document] (commit store document)) store documents))

;; TODO: eventual consistent backends: there could be more than one document for each id
;; They should all be kept in remote
;; We could promote one document (at random - or consistently - or by ownership)
;; and let merge take care of this (i.e. generate conflicts as needed). Or we could store
;; all documents in a vector. Or in a special map. In any case, a checkout should succeed. 
(defn fetch
  [store context fetcher]
  (let [context-name (:name context)
        external-ids (:external-ids context)
        boundaries (get-free-external-doctypes context)
        documents (fetcher context-name external-ids boundaries)]
    (reduce
     (fn [store document]
       (-> store
           (add-document-if-new document :stored)
           (add-to-remote document)))
     store
     documents)))

(defn conflicted?
  [store id]
  (let [starred (dissoc (get-document store id :starred) :_hirop)
        stored (dissoc (get-document store id :stored) :_hirop)
        baseline (dissoc (get-document store id :baseline) :_hirop)]
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
   #(htype (get % :stored))
   (map
    (fn [id]
      ;; TODO: make stored be a list (eventual consistency)
      {:stored (get-document store id :stored)
       :starred (get-document store id :starred)
       :baseline (get-document store id :baseline)})
   (get-conflicted-ids store))))

(defn merge-document
  ;; In case of fast-forward, we advance :rev, otherwise we don't.
  ;; TODO: make merging strategy a multimethod
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
             (keys (dissoc stored :_hirop)))
            ;; TODO: consider merging more of _hirop than just rev
            merged (if (conflicted? store id)
                     merged
                     (assoc-hrev merged (hrev stored)))]
        (add-document store merged :starred))
      store)))

(defn merge-remote
  ;; for every remote, merge into local
  ;; since local and remote share :stored, this effectively means:
  ;; for every remote that is starred, merge unconflicted fields from :stored to :starred
  ;; unconflicted means those fields in stored that are unchanged between baseline and starred
  ;; keep in local only those that are either starred (or locked?) and those that come from remote
  [store]
  (let [store (reduce merge-document store (select (partial starred? store) (:remote store)))]
    (update-in store [:local] #(union (select (partial starred? store) %) (:remote store)))))

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
                          (hrels doc)))
                new-doc (assoc-hid doc new-id)
                new-doc (assoc-hrels doc new-relations)]
            [new-id new-doc]))
        starred))))
   (update-in
    [:local]
    (fn [local]
      (->
       local
       (difference (set (keys tmp-map)))
       (union (set (vals tmp-map))))))
   ;; TODO: prototypes
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
  ;; TODO: prototypes
  [store doctype]
  (select #(= (name doctype) (name (htype (get-document store %)))) (:local store)))

(defn checkout
  ([store]
     (map #(get-document store %) (:local store)))
  ;; TODO: prototypes
  ([store doctype]
     (map #(get-document store %) (get-ids-of-type store doctype))))

(defn get-selected-ids
  ([store selection-id]
     (get-in store [:selections selection-id]))
  ;; TODO: prototypes
  ([store selection-id doctype]
     (get-in store [:selections selection-id doctype])))

(defn checkout-selected
  ;; TODO: prototypes
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
  ;; TODO: prototypes
  [context doctype direction]
  (condp = direction
      :out (filter (fn [rel] (= doctype (rel :from))) (context :relations))
      :in (filter (fn [rel] (= doctype (rel :to))) (context :relations))))

(defn- walk-relation
  ;; TODO: prototypes
  [store context selection-id doctype rel]
  (let [ids (get-in store [:selections selection-id doctype])
        [direction rel-doctype]
        (condp = doctype
          (:from rel) [:out (:to rel)]
          (:to rel) [:in (:from rel)])]
    (if (get-in context [:selections selection-id rel-doctype])
      (reduce
       (fn [store id]
         (let [values
               (condp = direction
                 :out (hrel (get-document store id) rel-doctype)
                 :in (hid (get-document store id)))
               values (if (coll? values) values (vector values))
               rel-ids
               (reduce
                (fn [out value]
                  (let [rel-ids
                        (filter
                         (fn [rel-id]
                           (let [rel-values
                                 (condp = direction
                                   :out rel-id
                                   :in (hrel (get-document store rel-id) doctype))]
                             (if (coll? rel-values)
                               (some #(= value %) rel-values)
                               (= value rel-values))))
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
       ids)
      store)))

(defn- propagate-selection
  ;; recursively propagate selection backward and forward, starting from id and making sure a document
  ;; type is not selected twice, return store with updated selection
  ;; TODO: there were hooks in the Javascript version that are called everytime a selection changes.
  ;; Here we could keep track of who changed at the end of the propagation (better, called once)
    ;; TODO: prototypes
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
         (let [store (reduce (fn [store rel] (walk-relation store context selection-id doctype rel)) store out)
               store (reduce (fn [store rel] (walk-relation store context selection-id doctype rel)) store in)]
           store)
         (concat (rest doctypes) (map :to out) (map :from in))
         (conj visited doctype))))))

;; TODO: support multiple selection
(defn select-document
  [store context id selection-id]
  (let [doctype (htype (get-document store id))]
    (-> store
        (assoc-in [:selections selection-id] nil)
        (assoc-in [:selections selection-id doctype] [id])
        (propagate-selection context selection-id [doctype]))))

(defn unselect
  ;; TODO: prototypes
  [store context selection-id doctype]
  (if doctype
    (-> store
        (assoc-in [:selections selection-id doctype] nil)
        (propagate-selection context selection-id [doctype]))
    (assoc-in store [:selections selection-id] nil)))

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

