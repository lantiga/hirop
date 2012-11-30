(ns hirop.core-test
  (:use clojure.test
        clojure.set
        hirop.core))

(def doctypes
  {:Foo {:fields {:id {}}}
   :Bar {:fields {:title {}}}
   :Baz {:fields {:title {}}}})

(def cardinality-test-context
  {:relations
   [{:from :Bar :to :Foo :external true :cardinality :one}
    {:from :Baz :to :Bar :cardinality :many}]
   :selections
   {:test
    {:Foo {:sort-by [:id] :select :last}
     :Bar {:select :all}
     :Baz {:select :all}}}
   :configurations {}})

(defn cardinality-test-fetcher [context-name external-ids boundaries]
  [{:_id "0" :_type :Foo :id "0"}
   {:_id "1" :_type :Bar :Foo_ "0" :title "First"}
   {:_id "2" :_type :Bar :Foo_ "0" :id "Second"}
   {:_id "3" :_type :Baz :Bar_ ["1" "2"] :title "Third"}])

(deftest cardinality-test
  (let [context (init-context :Test cardinality-test-context doctypes {} {:Foo "0"})
        store (new-store :Test {})
        store (fetch store context cardinality-test-fetcher)
        store (merge-remote store)
        store-baz (select-document store context "3" :test)
        store-foo (select-document store context "0" :test)]
    (is (= (get-in store-baz [:selections :test])
           {:Baz ["3"], :Bar ["1" "2"], :Foo ["0"]}))
    (is (= (get-in store-foo [:selections :test])
           {:Baz ["3"], :Bar ["1" "2"], :Foo ["0"]}))))



(def remap-test-context
  {:relations
   [{:from :Bar :to :Foo :external true :cardinality :one}
    {:from :Baz :to :Bar :cardinality :many}]
   :selections
   {:test
    {:Foo {:sort-by [:id] :select :last}
     :Bar {:select :all}
     :Baz {:select :all}}}
   :configurations {}})

(defn remap-test-fetcher [context-name external-ids boundaries]
  [{:_id "0" :_type :Foo :id "0"}])

(defn remap-test-saver [docs context-name]
  {:result :success :remap {"tmp1" "1" "tmp2" "2" "tmp3" "3"}})

(deftest remap-test
  (let [context (init-context :Test remap-test-context doctypes {} {:Foo "0"})
        store (new-store :Test {})
        store (fetch store context remap-test-fetcher)
        store (merge-remote store)
        store (inc-uuid store)
        id0 (get-uuid store)
        store (inc-uuid store)
        id1 (get-uuid store)
        store (inc-uuid store)
        id2 (get-uuid store)
        bar1 (merge (new-document context :Bar) {:_id id0 :Foo_ "0"})
        bar2 (merge (new-document context :Bar) {:_id id1 :Foo_ "0"})
        baz (merge (new-document context :Baz) {:_id id2 :Bar_ [id0 id1]})
        store (mcommit store [bar1 bar2 baz])
        store (select-document store context "0" :test)
        store (push store remap-test-saver)]
    (is (= (:local store)
           #{"0" "1" "2" "3"}))
    (is (= (:_id (first (checkout-selected store :test :Baz)))
           "3"))
    (is (= (:Bar_ (first (checkout store :Baz)))
           ["1" "2"]))))




(def select-all-test-context
  {:relations
   [{:from :Bar :to :Foo :external true :cardinality :one}
    {:from :Baz :to :Bar :cardinality :one}]
   :selections
   {:test-all
    {:Foo {:sort-by [:id] :select :last}
     :Bar {:select :all}
     :Baz {:select :all}}
    :test-first
    {:Foo {:sort-by [:id] :select :last}
     :Bar {:select :all}
     :Baz {:select :first}}}
   :configurations {}})

(defn select-all-test-fetcher [context-name external-ids boundaries]
  [{:_id "0" :_type :Foo :id "0"}
   {:_id "1" :_type :Bar :Foo_ "0" :title "First"}
   {:_id "2" :_type :Bar :Foo_ "0" :id "Second"}
   {:_id "3" :_type :Baz :Bar_ "2" :title "Third"}
   {:_id "4" :_type :Baz :Bar_ "2" :id "Fourth"}
   {:_id "5" :_type :Baz :Bar_ "1" :title "Fifth"}
   {:_id "6" :_type :Baz :Bar_ "1" :id "Sixth"}])

(deftest select-all-test
  (let [context (init-context :Test select-all-test-context doctypes {} {:Foo "0"})
        store (new-store :Test {})
        store (fetch store context select-all-test-fetcher)
        store (merge-remote store)
        store-all (select-document store context "0" :test-all)
        store-first (select-document store context "0" :test-first)]
    (is (= (get-in store-all [:selections :test-all])
           {:Baz ["5" "6" "3" "4"], :Bar ["1" "2"], :Foo ["0"]}))
    ;; test reselection
    (is (= (get-in (select-document store-all context "0" :test-all) [:selections :test-all])
           {:Baz ["5" "6" "3" "4"], :Bar ["1" "2"], :Foo ["0"]}))
    (is (= (get-in store-first [:selections :test-first])
           {:Baz ["5" "3"], :Bar ["1" "2"], :Foo ["0"]}))))


(def select-defaults-test-context
  {:relations
   [{:from :Bar :to :Foo :external true}
    {:from :Baz :to :Foo :external true}]
   :selections
   {:test-defaults
    {:Foo {}
     :Bar {:select :first}
     :Baz {:select :first}}}
   :configurations {}})

(defn select-defaults-test-fetcher [context-name external-ids boundaries]
  [{:_id "0" :_type :Foo :id "0"}])

(deftest select-defaults-test
  (let [context (init-context :Test select-defaults-test-context doctypes {} {:Foo "0"})
        store (new-store :Test {})
        store (fetch store context select-defaults-test-fetcher)
        store (merge-remote store)
        store (select-defaults store context :test-defaults)
        store (select-defaults store context :test-defaults)]
    (is (= {:Foo [{:_id "0", :id "0", :_type :Foo}]}
           (checkout-selected store :test-defaults)))))


(def conflict-test-context
  {:relations []
   :selections {}})

(defn conflict-test-fetcher-1 [context-name external-ids boundaries]
  [{:_id "0" :_type :Foo :_meta {} :id "0"}])

(defn conflict-test-fetcher-2 [context-name external-ids boundaries]
  [{:_id "0" :_type :Foo :_meta {} :id "0"} {:_id "1" :_type :Bar :_meta {} :title "" :Foo_ "0"}])

(defn conflict-test-fetcher-3 [context-name external-ids boundaries]
  [{:_id "0" :_type :Foo :_meta {} :id "0"} {:_id "1" :_type :Bar :_meta {} :title "BAR" :Foo_ "0"}])

(defn conflict-test-saver-1 [docs context-name]
  {:result :success :remap {"tmp1" "1"}})

(defn conflict-test-saver-2 [docs context-name]
  {:result :success :remap {}})

(deftest remap-test
  (let [context (init-context :Test remap-test-context doctypes {} {:Foo "0"})
        store (new-store :Test {})
        store (fetch store context conflict-test-fetcher-1)
        store (merge-remote store)
        store (inc-uuid store)
        id0 (get-uuid store)
        bar (merge (new-document context :Bar) {:_id id0 :Foo_ "0"})
        store (commit store bar)
        store (push store conflict-test-saver-1)
        store (fetch store context conflict-test-fetcher-2)
        store (merge-remote store)
        bar (first (checkout store :Bar))
        bar (assoc bar :title "BAR")
        store (commit store bar)
        store (push store conflict-test-saver-2)
        store (fetch store context conflict-test-fetcher-3)
        store (merge-remote store)]
    (is (empty? (checkout-conflicted store)))))
