(ns hirop.backend)

(defmulti save (fn [backend & _] backend))

(defmulti fetch (fn [backend & _] backend))

(defmulti history (fn [backend & _] backend))

