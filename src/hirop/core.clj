(ns hirop.core
  (:use [clojure.set :only [union select difference]])
  (:require [clojure.string :as string]))

(def tmp-prefix "tmp")

(defn uuid
  []
;*CLJSBUILD-REMOVE*; (apply str (map (fn [x] (if (= x \0) (.toString (bit-or (* 16 (.random js/Math)) 0) 16) x)) "00000000-0000-4000-0000-000000000000"))
;*CLJSBUILD-REMOVE*;#_
  (str (java.util.UUID/randomUUID)))

(defn tmp-uuid
  []
  (str tmp-prefix (uuid)))

(defn hid [doc]
  (get-in doc [:_hirop :id]))

(defn hrev [doc]
  (get-in doc [:_hirop :rev]))

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

(defn dissoc-hrev [doc]
  (update-in doc [:_hirop] dissoc :rev))

(defn assoc-hrels [doc rels]
  (assoc-in doc [:_hirop :rels] rels))

(defn dissoc-hrels [doc]
  (update-in doc [:_hirop] dissoc :rels))

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
  (string? (re-find (re-pattern (str "^" tmp-prefix)) id)))

(defn has-temporary-id?
  [doc]
  (is-temporary-id? (hid doc)))

(defn- get-relation-fields
  [context doctype]
  (reduce
   (fn [res el]
     (if (or
          (= doctype (:from el))
          (= :* (:from el)))
       (assoc res (:to el) el)
       res))
   {}
   (:relations context)))

(defn get-external-doctypes
  [context]
  (map :to (filter :external (:relations context))))

(defn get-free-external-doctypes
  [context]
  (filter #(not (contains? (:external-ids context) %)) (get-external-doctypes context)))

(defn- walk-prototype
  [prototypes prototype]
  (loop [prototype-list [prototype]
         doctypes []]
    (if (empty? prototype-list)
      doctypes
      (if-let [children (get prototypes (first prototype-list))]
        (recur (concat (rest prototype-list) children) doctypes)
        (recur (rest prototype-list) (conj doctypes (keyword (first prototype-list))))))))

(defn- compile-prototypes
  [prototypes]
  (into {}
        (map
         (fn [[k v]]
           [k (vec (distinct (flatten (map (partial walk-prototype prototypes) v))))])
         prototypes)))

(defn- compile-selections
  [selections prototypes]
  (into
   {}
   (map
    (fn [[k selection]]
      [k
       (reduce
        (fn [selection [d s]]
          (if-let [children (get prototypes d)]
            (let [selection (dissoc selection d)]
              (reduce (fn [out c] (assoc out c s)) selection children))
            selection))
        selection
        selection)
       ])
    selections)))

(defn- compile-relations
  [relations prototypes]
  (reduce
   (fn [out r]
     (let [from-p (or (get prototypes (:from r)) [(:from r)])
           to-p (or (get prototypes (:to r)) [(:to r)])]
       (reduce
        (fn [out fp]
          (reduce
           (fn [out tp]
             (conj out (-> r (assoc :from (keyword fp)) (assoc :to (keyword tp)))))
           out
           to-p))
        out
        from-p)))
   []
   relations))

(defn- document-store
  []
  {:push-result nil
   :merge-result nil
   :stored {}
   :starred {}
   :baseline {}
   :remote #{}
   :local #{}
   :revisions {}
   :selected {}})

;; TODO: here we should check that all relations marked as external are specified in external-ids, otherwise fail
;; Also, we should only include doctypes that are in use in the context (in relationships, selections or prototypes)
(defn init-context
  ([context-name context doctypes external-ids meta backend]
     (let [prototypes (compile-prototypes (:prototypes context))
           selections (compile-selections (:selections context) prototypes)
           relations (compile-relations (:relations context) prototypes)]
       (->
        context
        (merge {:name context-name
                :doctypes doctypes
                :prototypes prototypes
                :selections selections
                :relations relations
                :external-ids external-ids
                :meta meta
                :backend backend})
        (merge (document-store))))))

(defn get-doctype
  [context doctype]
  (let [configuration (get-in context [:configurations doctype])
        relations (get-relation-fields context doctype)]
    (->
     (get-in context [:doctypes doctype])
     (assoc :conf configuration)
     (assoc :rels relations))))

(def empty-document
  {:_hirop
   {:id nil
    :rev nil
    :meta nil
    :type nil
    :conf nil
    :rels nil}})

(defn new-document
  [context doctype]
  (let [fields (zipmap (keys (get-in context [:doctypes doctype :fields])) (repeat ""))
        configuration (get-in context [:configurations doctype])
        relations (get-relation-fields context doctype)
        relations (reduce
                   (fn [rels [k v]]
                     (if (contains? rels k)
                       (assoc rels k v)
                       rels))
                   (zipmap (keys relations) (repeat nil))
                   (get context :external-ids))]
    (->
     empty-document
     (assoc-hid (tmp-uuid))
     (assoc-htype doctype)
     (assoc-in [:_hirop :conf] configuration)
     (assoc-hrels relations)
     (merge fields))))

(defn- get-id
  [doc-or-id]
  (if (string? doc-or-id)
    doc-or-id
    (hid doc-or-id)))

(defn get-document
  ([context id]
     (let [starred (get-in context [:starred id])
           stored (get-in context [:stored id])]
       (cond
        starred starred
        :else stored)))
  ([context id bucket]
     (get-in context [bucket id])))

(defn get-stored
  [context id]
  (get-in context [:stored id]))

(defn get-baseline
  [context id]
  (let [baseline (get-in context [:baseline id])
        stored (get-in context [:stored id])]
    (cond
     baseline baseline
     :else stored)))

(defn document-state
  [context id]
  (let [starred (get-in context [:starred id])
        stored (get-in context [:stored id])]
    (cond
     starred :starred
     :else :stored)))

(defn starred?
  [context id]
  (= :starred (document-state context id)))

(defn stored?
  [context id]
  (= :stored (document-state context id)))

(defn revisions
  [context id]
  (vec (get-in context [:revisions id])))

(defn add-document
  ([context document]
     (add-document context document :stored))
  ([context document bucket]
     (->
      context
      (assoc-in [bucket (hid document)] document)
      (update-in [:revisions (hid document)]
                 (fn [revs rev] (let [revs (or revs #{})] (if rev (conj revs rev) revs)))
                 (hrev document)))))

(defn add-document-if-new
  ([context document]
     (add-document-if-new context document :stored))
  ([context document bucket]
     (if (contains? (revisions context (hid document)) (hrev document))
       context
       (add-document context document bucket))))

(defn add-to-set
  [context document set-name]
  (update-in context [set-name] conj (hid document)))

(defn add-to-local
  [context document]
  (add-to-set context document :local))

(defn add-to-remote
  [context document]
  (add-to-set context document :remote))

(defn commit
  [context document]
  ;; In a multi-threaded environment, this should be atomic (like all the others, in any case)
  ;; Here baseline could already be at a greater revision number compared to the committed document
  ;; had the latter been checked out some time before the commit.
  (let [baseline (get-stored context (hid document))
        timestamp
        ;*CLJSBUILD-REMOVE*;(.toISOString (js/Date.))
        ;*CLJSBUILD-REMOVE*;#_
        (.. (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ssZ") (format (java.util.Date.)))
        document (assoc-hmeta document
                              (merge (hmeta document) (:meta context) {:timestamp timestamp}))
        document (if (hid document) document (assoc-hid document (tmp-uuid)))
        context (if baseline (add-document context baseline :baseline) context)]
    (-> context
        (add-document document :starred)
        (add-to-local document))))

(defn mcommit
  [context documents]
  (reduce (fn [context document] (commit context document)) context documents))

;; TODO: eventual consistent backends: there could be more than one document for each id
;; They should all be kept in remote
;; We could promote one document (at random - or consistently - or by ownership)
;; and let merge take care of this (i.e. generate conflicts as needed). Or we could store
;; all documents in a vector. Or in a special map. In any case, a checkout should succeed.
(defn fetch
  [context fetcher]
  (let [{documents :documents context-info :context-info} (fetcher context)]
    (reduce
     (fn [context document]
       (-> context
           (add-document-if-new document :stored)
           (add-to-remote document)))
     (assoc context :context-info context-info) 
     documents)))

(defn conflicted?
  [context id]
  ;; TODO: when merging strategy is pluggable, use the
  ;; same predicate
  (if-let [starred (get-document context id :starred)]
    (let [stored (get-document context id :stored)
          baseline (get-document context id :baseline)
          starred-fields (dissoc starred :_hirop)
          stored-fields (dissoc stored :_hirop)
          baseline-fields (dissoc baseline :_hirop)
          starred-rels (hrels starred)
          stored-rels (hrels stored)
          baseline-rels (hrels baseline)]
      (and
       (not= (hrev starred) (hrev stored))
       (or
        (and
         (not= starred-fields baseline-fields)
         (not= stored-fields baseline-fields))
        (and
         (not= starred-rels baseline-rels)
         (not= stored-rels baseline-rels)))))
    false))

(defn get-conflicted-ids
  [context]
  (filter (partial conflicted? context) (:local context)))

(defn any-conflicted?
  [context]
  (not (empty? (get-conflicted-ids context))))

(defn checkout-conflicted
  [context]
  (group-by
   #(htype (get % :stored))
   (map
    (fn [id]
      ;; TODO: make stored be a list (eventual consistency)
      {:stored (get-document context id :stored)
       :starred (get-document context id :starred)
       :baseline (get-document context id :baseline)})
   (get-conflicted-ids context))))

(defn commit-conflicted
  [context document]
  (let [stored-rev (hrev (get-stored context (hid document)))
        document (assoc-hrev document stored-rev)]
    (commit context document)))

(defn mcommit-conflicted
  [context documents]
  (reduce (fn [context document] (commit-conflicted context document)) context documents))

(defn merge-document
  ;; In case of fast-forward, we advance :rev, otherwise we don't.
  [context id]
  (let [starred (get-document context id :starred)
        stored (get-document context id :stored)
        baseline (get-document context id :baseline)]
    (if (and starred (not= (hrev baseline) (hrev stored)))
      ;; merging strategy - make it pluggable
      (let [merged
            (reduce
             (fn [document key]
               (if (not= (get baseline key) (get starred key))
                 document
                 (assoc document key (get stored key))))
             starred
             (keys (dissoc stored :_hirop)))
            merged
            (assoc-hrels
             merged
             (reduce
              (fn [rels rel-doctype]
                (if (not= (hrel baseline rel-doctype) (hrel starred rel-doctype))
                  rels
                  (assoc rels rel-doctype (hrel stored rel-doctype))))
              (hrels starred)
              (keys (hrels stored))))
            ;; TODO: consider merging more of _hirop than just rev
            merged (if (conflicted? context id)
                     merged
                     (assoc-hrev merged (hrev stored)))]
        (add-document context merged :starred))
      context)))

(defn merge-remote
  ;; for every remote, merge into local
  ;; since local and remote share :stored, this effectively means:
  ;; for every remote that is starred, merge unconflicted fields from :stored to :starred
  ;; unconflicted means those fields in stored that are unchanged between baseline and starred
  ;; keep in local only those that are either starred (or locked?) and those that come from remote
  [context]
  (let [context (reduce merge-document context (select (partial starred? context) (:remote context)))]
    (update-in context [:local] #(union (select (partial starred? context) %) (:remote context)))))

(defn pull
  [context fetcher]
  (-> context (fetch fetcher) merge-remote))

(defn- unstar
  ;; move starred to stored (really?), remove baselines (locked?)
  ([context]
    (unstar context (keys (:starred context))))
  ([context starred-ids]
     (-> context
         (update-in [:stored] merge (select-keys (:starred context) starred-ids))
         ;; this only removes the starred and baselines relative to the provided starred
         (update-in [:starred] #(apply dissoc % starred-ids))
         (update-in [:baseline] #(apply dissoc % starred-ids)))))

(defn- set-merge-result
  [context result]
  (assoc context :merge-result result))

(defn- set-push-result
  [context result]
  (assoc context :push-result result))

(defn get-push-result
  [context]
  (:push-result context))

(defn- remap-tmp-ids
  [context tmp-map]
  (->
   context
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
                new-doc (-> doc (assoc-hid new-id) (assoc-hrels new-relations))]
            [new-id new-doc]))
        starred))))
   (update-in
    [:local]
    (fn [local]
      (->
       local
       (difference (set (keys tmp-map)))
       (union (set (vals tmp-map))))))
   (update-in
    [:revisions]
    (fn [revisions]
      (into
       {}
       (map
        (fn [[id revs]]
          [(or (get tmp-map id) id) revs])
        revisions))))
   (assoc :selected
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
       (:selected context))))))

(defn push-save
  [context saver]
  (let [save-ret (saver context)]
    {:save-ret save-ret
     :starred (:starred context)}))

(defn push-post-save
  ;; this is decoupled from push to provide a side-effect free option
  ;; Also, save-info contains starred, which typically are the starred
  ;; that were considered in the save.
  [context save-info]
  (let [context
        (if (= :success (get-in save-info [:save-ret :result]))
          (->
           context
           (assoc :context-info (get-in save-info [:save-ret :context-info]))
           (remap-tmp-ids (get-in save-info [:save-ret :remap]))
           (unstar (map #(get (get-in save-info [:save-ret :remap]) % %) (keys (:starred save-info)))))
          context)]
    (set-push-result context (get-in save-info [:save-ret :result]))))

(defn push
  ;; save, upon success unstar, otherwise return context unmodified
  ;; leave it to a higher order function to reiterate
  [context saver]
  (let [ret (push-save context saver)]
    (push-post-save context ret)))

(defn- prototype-doctypes
  [prototypes doctype]
  (or (get prototypes doctype) [doctype]))

(defn- get-ids-of-type
  [context doctype]
  (let [doctype (keyword doctype)
        doctype-set (set (prototype-doctypes (:prototypes context) doctype))]
    (select #(contains? doctype-set (htype (get-document context %))) (:local context))))

(defn get-prototype-doctypes
  [context prototype]
  (get (:prototypes context) prototype))

(defn checkout
  ([context]
     (map #(get-document context %) (:local context)))
  ([context doctype]
     (map #(get-document context %) (get-ids-of-type context doctype))))

(defn get-selected-ids
  ([context selection-id]
     (get-in context [:selected selection-id]))
  ([context selection-id doctype]
     (let [doctypes (prototype-doctypes (:prototypes context) doctype)]
       (->>
        (map #(get-in context [:selected selection-id %]) doctypes)
        (filter #((comp not nil?) %))
        flatten))))

(defn checkout-selected
  ([context selection-id doctype]
     (let [ids (get-selected-ids context selection-id doctype)]
       (if (string? ids)
         (get-document context ids)
         (map #(get-document context %) ids))))
  ([context selection-id]
     (into {}
           (map
            (fn [[doctype _]] [doctype (checkout-selected context selection-id doctype)])
            (get-selected-ids context selection-id)))))

;; TODO: upon loading the context, create the relations graph for efficiency
(defn- get-relations
  [context doctype direction]
  (condp = direction
      :out (filter (fn [rel] (= doctype (rel :from))) (context :relations))
      :in (filter (fn [rel] (= doctype (rel :to))) (context :relations))))

(defn- walk-relation
  [context selection-id doctype rel]
  (let [ids (get-in context [:selected selection-id doctype])
        [direction rel-doctype]
        (condp = doctype
          (:from rel) [:out (:to rel)]
          (:to rel) [:in (:from rel)])]
    (if (get-in context [:selections selection-id rel-doctype])
      (reduce
       (fn [context id]
         (let [values
               (condp = direction
                 :out (hrel (get-document context id) rel-doctype)
                 :in (hid (get-document context id)))
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
                                   :in (hrel (get-document context rel-id) doctype))]
                             (if (coll? rel-values)
                               (some #(= value %) rel-values)
                               (= value rel-values))))
                         (get-ids-of-type context rel-doctype))
                        rel-ids (if-let [sort-keys (get-in context [:selections selection-id rel-doctype :sort-by])]
                                  (let [sort-keys (vec (map keyword sort-keys))]
                                    (sort-by (fn [el] ((apply juxt sort-keys) (get-document context el))) rel-ids))
                                  rel-ids)
                        rel-ids (condp = (keyword (get-in context [:selections selection-id rel-doctype :select]))
                                  :first (if (first rel-ids) [(first rel-ids)] [])
                                  :last (if (last rel-ids) [(last rel-ids)] [])
                                  :all rel-ids
                                  rel-ids)]
                    (concat out rel-ids)))
                []
                values)]
           (if (some empty? [values rel-ids])
             context
             (update-in context [:selected selection-id rel-doctype] #(vec (distinct (concat % rel-ids)))))))
       context
       ids)
      context)))

(defn- propagate-selection
  ;; recursively propagate selection backward and forward, starting from id and making sure a document
  ;; type is not selected twice, return context with updated selection
  ;; TODO: there were hooks in the Javascript version that are called everytime a selection changes.
  ;; Here we could keep track of who changed at the end of the propagation (better, called once)
  [context selection-id doctypes]
  (loop [context context
         doctypes doctypes
         visited (into #{} doctypes)]
    (if (empty? doctypes)
      context
      (let [doctype (first doctypes)
            out (filter #(not (contains? visited (:to %))) (get-relations context doctype :out))
            in (filter #(not (contains? visited (:from %))) (get-relations context doctype :in))]
        (recur
         (let [context (reduce (fn [context rel] (walk-relation context selection-id doctype rel)) context out)
               context (reduce (fn [context rel] (walk-relation context selection-id doctype rel)) context in)]
           context)
         (concat (rest doctypes) (map :to out) (map :from in))
         (conj visited doctype))))))

;; TODO: support multiple selection
(defn select-document
  [context id selection-id]
  (let [doctype (htype (get-document context id))]
    (-> context
        (assoc-in [:selected selection-id] nil)
        (assoc-in [:selected selection-id doctype] [id])
        (propagate-selection selection-id [doctype]))))

(defn unselect
  [context selection-id doctype]
  (if doctype
    (-> context
        (assoc-in [:selected selection-id doctype] nil)
        (propagate-selection selection-id [doctype]))
    (assoc-in context [:selected selection-id] nil)))

;; TODO: support multiple external-ids of the same type
(defn select-defaults
  [context selection-id]
  (let [context (assoc-in context [:selected selection-id] nil)
        context
        (reduce
         (fn [context [external-doctype external-id]]
           (assoc-in context [:selected selection-id external-doctype] [external-id]))
         context
         (:external-ids context))]
    (propagate-selection context selection-id (keys (:external-ids context)))))
