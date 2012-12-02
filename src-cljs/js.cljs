(ns hirop.js
  (:use [hirop.js-util :only [clj->js]]
        [hirop.session :only [*hirop-session*]])
  (:require [hirop.stateful :as hirop]))

(def session (atom {}))

(defn- js-f [f & args]
  (let [clj-args (map js->clj args)]
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
    (hirop/init foo-doctypes {:foo foo-context} :foo)))


(defn ^:export init [doctypes contexts backend]
  (js-f hirop/init doctypes contexts backend))

(defn ^:export contexts []
  (js-f hirop/contexts))

(defn ^:export doctypes []
  (js-f hirop/doctypesB))

(defn ^:export clean-context []
  (js-f hirop/clean-context))

(defn ^:export init-context [context-name external-ids meta]
  (js-f hirop/init-context context-name external-ids meta))

(defn ^:export new-document [doctype]
  (js-f hirop/new-document doctype))

(defn ^:export new-documents [doctype-map]
  (js-f hirop/new-documents doctype-map))

(defn ^:export get-document [id]
  (js-f hirop/get-document id))

(defn ^:export get-external-documents []
  (js-f hirop/get-external-documents))

(defn ^:export get-configurations []
  (js-f hirop/get-configurations))

(defn ^:export get-configuration [doctype]
  (js-f hirop/get-configuration doctype))

(defn ^:export get-doctype [doctype]
  (js-f hirop/get-doctype doctype))

(defn ^:export get-baseline [id]
  (js-f hirop/get-baseline id))

(defn ^:export commit [document]
  (js-f hirop/commit document))

(defn ^:export mcommit [documents]
  (js-f hirop/mcommit documents))

(defn ^:export pull []
  (js-f hirop/pull))

(defn ^:export get-conflicted-ids []
  (js-f hirop/get-conflicted-ids))

(defn ^:export any-conflicted []
  (js-f hirop/any-conflicted))

(defn ^:export checkout-conflicted []
  (js-f hirop/checkout-conflicted))

(defn ^:export push []
  (js-f hirop/push))

(defn ^:export save [documents]
  (js-f hirop/save documents))

(defn ^:export history [id]
  (js-f hirop/history id))

(defn ^:export checkout
  ([]
     (js-f hirop/checkout))
  ([doctype]
     (js-f hirop/checkout doctype)))

(defn ^:export get-selected-ids
  ([selection-id]
     (js-f hirop/get-selected-ids selection-id))
  ([selection-id doctype]
     (js-f hirop/get-selected-ids selection-id doctype)))

(defn ^:export checkout-selected
  ([selection-id]
     (js-f hirop/checkout-selected selection-id))
  ([selection-id doctype]
     (js-f hirop/checkout-selected selection-id doctype)))

(defn ^:export select
  ([selection-id]
     (js-f hirop/select selection-id))
  ([selection-id id]
     (js-f hirop/select selection-id id)))

(defn ^:export unselect [selection-id doctype]
  (js-f hirop/unselect selection-id doctype))

