(ns hirop.atom-store
  (:use [hirop.core :only [uuid]] 
        [hirop.protocols :only [IContextStore]]))

(defn- dissoc-expired [store]
  (let [t (System/currentTimeMillis)]
    (loop [store store]
      (if (or (empty? (:ttl-set store)) (<= t (ffirst (:ttl-set store))))
        store
        (recur
         (let [el (first (:ttl-set store))
               context-id (second el)]
           (->
            store
            (update-in [:ttl-set] disj el)
            (update-in [:ttl-map] dissoc context-id)
            (update-in [:contexts] dissoc context-id))))))))

(defn- update-ttl [store expiration context-id]
  (let [ttl (+ (System/currentTimeMillis) (* 1000 expiration))
        prev-ttl (get-in store [:ttl-map context-id])]
    (->
     store
     (update-in [:ttl-set] disj [prev-ttl context-id])
     (update-in [:ttl-set] conj [ttl context-id])
     (update-in [:ttl-map] assoc context-id ttl))))

(defn- assoc-in-store [store expiration context-id v]
  (->
   (assoc-in store [:contexts context-id] v)
   (update-ttl expiration context-id)
   (dissoc-expired)))

(defn- update-in-store [store expiration context-id f]
  (let [store (dissoc-expired store)]
    (if (contains? (get store :contexts) context-id)
      (->
       (update-in store [:contexts context-id] f)
       (update-ttl expiration context-id))
      store)))

(defn- dissoc-in-store [store expiration context-id]
  (let [ttl (get-in store [:ttl context-id])]
    (->
     (update-in store [:contexts] dissoc context-id)
     (update-in [:ttl-set] disj [ttl context-id])
     (update-in [:ttl-map] dissoc context-id)       
     (dissoc-expired))))

(deftype AtomStore [a expiration]  
  IContextStore
  (get-context [_ context-id]
    (get-in @a [:contexts context-id]))
  
  (put-context [_ context]
    (let [context-id (uuid)]
      (swap! a assoc-in-store expiration context-id context)
      context-id))
  
  (assoc-context [_ context-id context]
    (swap! a assoc-in-store expiration context-id context)
    nil)
  
  (delete-context [_ context-id]
    (swap! a dissoc-in-store expiration context-id)
    nil)

  (update-context [_ context-id f]
    (->
     (swap! a update-in-store expiration context-id f)
     (get-in [:contexts context-id]))))

(defn atom-store
  "Return empty atom-based context store"
  [& {:keys [expiration-secs]
      :or {expiration-secs  (* 60 60)}}]
  (AtomStore. (atom {:ttl-set (sorted-set) :ttl-map {} :contexts {}}) expiration-secs))
