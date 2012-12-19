(ns hirop.backend)

(defmulti fetch (fn [backend context] backend))

(defmulti save (fn [backend store context] backend))

(defmulti history (fn [backend id] backend))

