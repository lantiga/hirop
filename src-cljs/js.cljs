(ns hirop.js
  (:use [hirop.js-util :only [clj->js]]
        [hirop.session :only [*hirop-session*]])
  (:require [hirop.stateful :as hirop]))

(def session (atom {}))

(defn- js-f [f & args]
  (let [clj-args (map js->clj args)]
    (binding [*hirop-session* session]
      (clj->js (apply f clj-args)))))

(defn- context-f [context-id f & args]
  (let [clj-args (map js->clj args)
        clj-args (if context-id
                   (concat clj-args [:context-id (js->clj context-id)])
                   clj-args)]
    (binding [*hirop-session* session]
      (clj->js (apply f clj-args)))))

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

(defn ^:export test-init []
  (binding [*hirop-session* session]
    (hirop/init test-doctypes {:foo test-context} :foo)))


(defn ^:export init [doctypes contexts backend]
  (js-f hirop/init doctypes contexts backend))

(defn ^:export contexts []
  (js-f hirop/contexts))

(defn ^:export doctypes []
  (js-f hirop/doctypes))

(defn ^:export clean-contexts []
  (js-f hirop/clean-contexts))

(defn ^:export clean-context [& [context-id]]
  (context-f context-id hirop/clean-context))

(defn ^:export create-context [context-name external-ids meta & [context-id]]
  (context-f context-id hirop/create-context context-name external-ids meta))

(defn ^:export new-document [doctype & [context-id]]
  (context-f context-id hirop/new-document doctype))

(defn ^:export new-documents [doctype-map & [context-id]]
  (context-f context-id hirop/new-documents doctype-map))

(defn ^:export get-document [id & [context-id]]
  (context-f context-id hirop/get-document id))

(defn ^:export get-external-documents [& [context-id]]
  (context-f context-id hirop/get-external-documents))

(defn ^:export get-configurations [& [context-id]]
  (context-f context-id hirop/get-configurations))

(defn ^:export get-configuration [doctype & [context-id]]
  (context-f context-id hirop/get-configuration doctype))

(defn ^:export get-doctype [doctype & [context-id]]
  (context-f context-id hirop/get-doctype doctype))

(defn ^:export get-baseline [id & [context-id]]
  (context-f context-id hirop/get-baseline id))

(defn ^:export commit [document & [context-id]]
  (context-f context-id hirop/commit document))

(defn ^:export mcommit [documents & [context-id]]
  (context-f context-id hirop/mcommit documents))

(defn ^:export pull [& [context-id]]
  (context-f context-id hirop/pull))

(defn ^:export get-conflicted-ids [& [context-id]]
  (context-f context-id hirop/get-conflicted-ids))

(defn ^:export any-conflicted [& [context-id]]
  (context-f context-id hirop/any-conflicted))

(defn ^:export checkout-conflicted [& [context-id]]
  (context-f context-id hirop/checkout-conflicted))

(defn ^:export push [& [context-id]]
  (context-f context-id hirop/push))

(defn ^:export save [documents & [context-id]]
  (context-f context-id hirop/save documents))

(defn ^:export history [id & [context-id]]
  (context-f context-id hirop/history id))

(defn ^:export checkout [& [doctype context-id]]
  (context-f context-id hirop/checkout :doctype doctype))

(defn ^:export get-selected-ids [& [doctype selection-id context-id]]
  (context-f context-id hirop/get-selected-ids :selection-id selection-id :doctype doctype))

(defn ^:export checkout-selected [& [doctype selection-id context-id]]
  (context-f context-id hirop/checkout-selected :selection-id selection-id :doctype doctype))

(defn ^:export select [& [id selection-id context-id]]
  (context-f context-id hirop/select :selection-id selection-id :id id))

(defn ^:export unselect [& [doctype selection-id context-id]]
  (context-f context-id hirop/unselect :selection-id selection-id :doctype doctype))

