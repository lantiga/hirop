(ns hirop.atom-store
  (:use hirop.core
        hirop.protocols))

;; TODO: make keys expiring (e.g. every hour). Reset timer at every modification of that key.

(deftype AtomStore [a]
  IContextStore
  (get-context [_ context-id]
    (get @a context-id))
  
  (put-context [_ context]
    (let [context-id (uuid)]
      (swap! a #(assoc % context-id context))
      context-id))
  
  (assoc-context [_ context context-id]
    (swap! a #(assoc % context-id context))
    nil)
  
  (delete-context [_ context-id]
    (swap! a #(dissoc % context-id))
    nil)

  (update-context [_ context-id f]
    (->
     (swap! a #(update-in % [context-id] f))
     (get context-id))))

(defn atom-store
  "Return empty atom-based context store"
  []
  (AtomStore. (atom {})))