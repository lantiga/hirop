(ns hirop.js
  (:use [hirop.js-util :only [clj->js]]
        [hirop.atom-store :only [atom-store]]
        [hirop.protocols :only [put-context get-context update-context]])
  (:require [hirop.core :as hirop]
            [hirop.protocols :as p]
            [hirop.backend :as backend]))

(def hirop-store (atom-store))

(def hirop-conf (atom {}))

;; TODO: PouchDb backend

(defn- set-hirop-conf!
  [doctypes contexts backend]
  (swap! hirop-conf assoc :doctypes doctypes :contexts contexts :backend backend))

(defn- init-context-in-store
  [context-name external-ids meta-info]
  (let [context-name (keyword context-name)]
    (put-context
      hirop-store
      (hirop/init-context context-name 
                          (get-in @hirop-conf [:contexts context-name]) 
                          (:doctypes @hirop-conf) 
                          (:external-ids @hirop-conf) 
                          meta-info 
                          (:backend @hirop-conf)))))

(def test-doctypes
  {:Foo {:fields {:id {}}}
   :Bar {:fields {:title {}}}
   :Baz {:fields {:title {}}}})

(def test-context
  {:relations
   [{:from :Bar :to :Foo :external true :cardinality :one}
    {:from :Baz :to :Bar :cardinality :many}]
   :selections
   {:test
    {:Foo {:sort-by [:id] :select :last}
     :Bar {:select :all}
     :Baz {:select :all}}}
   :configurations {}})

(defn ^:export set-conf 
  [doctypes contexts backend]
  (set-hirop-conf! (js->clj doctypes) (js->clj contexts) (js->clj backend)))

(defn ^:export init-context 
  [context-name external-ids]
  (init-context-in-store context-name (js->clj external-ids) {}))

(defn ^:export delete-context 
  [context-id]
  (p/delete-context hirop-store context-id))

(defn ^:export push 
  [context-id]
  (->>
    (update-context hirop-store context-id #(hirop/push % (partial backend/save (:backend @hirop-conf))))
    hirop/get-push-result
    (assoc {} :result)
    clj->js))

(defn ^:export pull 
  [context-id]
  (let [context (update-context hirop-store context-id 
                                #(hirop/pull % (partial backend/fetch (:backend @hirop-conf))))]
    (clj->js
      {:result (if (hirop/any-conflicted? context)) :conflict :success})))

(defn ^:export get-external 
  [context-id]
  (-> 
    (hirop/get-context hirop-store context-id)
    (get :external-ids)
    clj->js))

(defn ^:export get-doctypes 
  [context-id]
  (let [context (get-context hirop-store context-id)]
    (->>
      (map (fn [doctype] [doctype (hirop/get-doctype context (keyword doctype))]) (keys (get context :doctypes)))
      (into {})
      clj->js)))

(defn ^:export get-doctype 
  [context-id doctype]
  (->
    (get-context hirop-store context-id)
    (hirop/get-doctype (keyword doctype))
    clj->js))

(defn ^:export get-prototype-doctypes 
  [context-id prototype]
  (->
    (get-context hirop-store context-id)
    (hirop/get-prototype-doctypes (keyword prototype))
    clj->js))

(defn ^:export get-current
  [context-id doc-id]
  (->
    (get-context hirop-store context-id)
    (hirop/get-document doc-id)
    clj->js))

(defn ^:export get-baseline
  [context-id doc-id]
  (->
    (get-context hirop-store context-id)
    (hirop/get-baseline doc-id)
    clj->js))

(defn ^:export get-stored
  [context-id doc-id]
  (->
    (get-context hirop-store context-id)
    (hirop/get-stored doc-id)
    clj->js))

(defn ^:export get-history
  [context-id doc-id]
  (->
    (get-context hirop-store context-id)
    (backend/history (:backend @hirop-conf) doc-id)
    clj->js))

(defn ^:export any-conflicted
  [context-id]
  (->
    (get-context hirop-store context-id)
    (hirop/any-conflicted?)
    clj->js))

(defn ^:export get-conflicted
  [context-id]
  (->
    (get-context hirop-store context-id)
    (hirop/checkout-conflicted)
    clj->js))

(defn ^:export commit-conflicted
  [context-id doc-or-docs]
  (let [doc-or-docs (js->clj doc-or-docs)]
    (if (vector? doc-or-docs)
      (do
        (update-context hirop-store context-id #(hirop/mcommit-conflicted % doc-or-docs))
        (count doc-or-docs)) 
      (do 
        (update-context hirop-store context-id #(hirop/commit-conflicted % doc-or-docs))
        1))))

(defn ^:export get-new-document
  [context-id doctype]  
  (->
    (get-context hirop-store context-id)
    (hirop/new-document (keyword doctype))
    clj->js))

(defn ^:export commit
  [context-id doc-or-docs]
  (let [doc-or-docs (js->clj doc-or-docs)]
    (if (vector? doc-or-docs)
      (do
        (update-context hirop-store context-id #(hirop/mcommit % doc-or-docs))
        (count doc-or-docs)) 
      (do 
        (update-context hirop-store context-id #(hirop/commit % doc-or-docs))
        1))))

(defn ^:export checkout
  ([context-id]
   (->
     (get-context hirop-store context-id)
     hirop/checkout
     clj->js))
  ([context-id doctype]
   (->
     (get-context hirop-store context-id)
     (hirop/checkout (keyword doctype))
     clj->js)))

(defn ^:export checkout-selected
  ([context-id selection-id]
   (->
     (get-context hirop-store context-id)
     (hirop/checkout-selected (keyword selection-id)) 
     clj->js))
  ([context-id selection-id doctype]
   (->
     (get-context hirop-store context-id)
     (hirop/checkout-selected (keyword selection-id) (keyword doctype))
     clj->js)))

(defn ^:export get-selected-ids
  ([context-id selection-id]
   (->
     (get-context hirop-store context-id)
     (hirop/get-selected-ids (keyword selection-id)) 
     clj->js))
  ([context-id selection-id doctype]
   (->
     (get-context hirop-store context-id)
     (hirop/get-selected-ids (keyword selection-id) (keyword doctype)) 
     clj->js)))

(defn ^:export select-defaults
  [context-id selection-id]
  (update-context hirop-store context-id #(hirop/select-defaults % (keyword selection-id))))

(defn ^:export select-document
  [context-id selection-id doc-id]
  (update-context hirop-store context-id #(hirop/select-document % doc-id (keyword selection-id))))

(defn ^:export unselect
  [context-id selection-id doctype]
  (update-context hirop-store context-id #(hirop/unselect % (keyword selection-id) (keyword doctype))))

