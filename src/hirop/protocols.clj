(ns hirop.protocols
  (:require [hirop.core :as core]))

(defprotocol IContextStore
  "The protocol for hirop context stores."
  ;; an atom, a megaref, a redis store, ...

  (get-context [this context-id]
    "Get context from store by context-id, return context")
  
  (put-context [this context]
    "Insert context in store, return context-id")
  
  (assoc-context [this context context-id]
    "Insert context in store, return nil")

  (delete-context [this context-id]
    "Remove context from store by context-id, return nil")

  ;; For a Redis store (i.e. when not in memory) this could execute in a transaction and update
  ;; only those fields of a hash for which there is a change
  (update-context [this context-id f]
    "Execute f taking the context at context-id in input and store the result at context-id, return new context"))

;; In addition, there should be a context protocol, for use directly in store.
