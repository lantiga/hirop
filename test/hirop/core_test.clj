(ns hirop.core-test
  (:use clojure.test
        clojure.set
        clojure.pprint
        hirop.core))

(def doctypes
  {:Foo {:fields {:id {}}}
   :Bar {:fields {:title {}}}
   :Baz {:fields {:title {}}}
   :Baq {:fields {:title {}}}})

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

(defn cardinality-test-fetcher [_]
  [{:_hirop {:id "0" :type :Foo}
    :id "0"}
   {:_hirop {:id "1" :type :Bar :rels {:Foo "0"}}
    :title "First"}
   {:_hirop {:id "2" :type :Bar :rels {:Foo "0"}}
    :title "Second"}
   {:_hirop {:id "3" :type :Baz :rels {:Bar ["1" "2"]}}
    :title "Third"}])

(deftest cardinality-test
  (let [context
        (->
         (create-context :Test cardinality-test-context doctypes {:Foo "0"} {} :none)
         (fetch cardinality-test-fetcher)
         (merge-remote))
        context-baz (select-document context "3" :test)
        context-foo (select-document context "0" :test)]
    (is (= (get-in context-baz [:selected :test])
           {:Baz ["3"], :Bar ["1" "2"], :Foo ["0"]}))
    (is (= (get-in context-foo [:selected :test])
           {:Baz ["3"], :Bar ["1" "2"], :Foo ["0"]}))))


(def prototype-test-context
  {:prototypes
   {:Foobar [:Foo :Bar]
    :Foobaz [:Foo :Baz]
    :Foobarbaz [:Foobar :Foobaz]
    :Barbaz [:Bar :Baz]}
   :relations
   [{:from :Barbaz :to :Foo}]
   :selections
   {:test
    {:Foo {:sort-by [:id] :select :last}
     :Barbaz {:select :all}}}
   :configurations {}})

(defn prototype-test-fetcher [_]
  [{:_hirop {:id "0" :type :Foo}
    :id "0"}
   {:_hirop {:id "1" :type :Bar :rels {:Foo "0"}}
    :title "First"}
   {:_hirop {:id "2" :type :Bar :rels {:Foo "0"}}
    :title "Second"}
   {:_hirop {:id "3" :type :Baz :rels {:Foo "0"}}
    :title "Third"}
   {:_hirop {:id "4" :type :Baz :rels {:Foo "0"}}
    :title "Fourth"}])

(deftest prototype-test
  (let [context
        (->
         (create-context :Test prototype-test-context doctypes {:Foo "0"} {} :none)
         (fetch prototype-test-fetcher)
         (merge-remote))
        context-baz (select-document context "3" :test)
        context-foo (select-document context "0" :test)]
    (is (empty?
         (filter #(= :Foo (htype %)) (checkout-selected context-baz :test :Barbaz))))
    (is (= (get-in context-baz [:selected :test])
           {:Baz ["3"], :Bar ["1" "2"], :Foo ["0"]}))
    (is (= (get-in context-foo [:selected :test])
           {:Baz ["3" "4"], :Bar ["1" "2"], :Foo ["0"]}))))


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

(defn remap-test-fetcher [_]
  [{:_hirop {:id "0" :type :Foo}
    :id "0"}])

(defn remap-test-saver [_]
  {:result :success :remap {"tmp1" "1" "tmp2" "2" "tmp3" "3"}})

(deftest remap-test
  (let [context
        (->
         (create-context :Test remap-test-context doctypes {:Foo "0"} {} :none)
         (fetch remap-test-fetcher)
         (merge-remote))
        bar1 (assoc (new-document context :Bar) :_hirop {:rels {:Foo "0"}})
        bar2 (assoc (new-document context :Bar) :_hirop {:rels {:Foo "0"}})
        baz (assoc (new-document context :Baz) :_hirop {:rels {:Bar [(hid bar1) (hid bar2)]}})
        context (mcommit context [bar1 bar2 baz])
        context (select-document context "0" :test)
        context (push context remap-test-saver)]
    (is (= (:local context)
           #{"0" "1" "2" "3"}))
    (is (= (:_id (first (checkout-selected context :test :Baz)))
           "3"))
    (is (= (:Bar_ (first (checkout context :Baz)))
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

(defn select-all-test-fetcher [_]
  [{:_hirop {:id "0" :type :Foo}
    :id "0"}
   {:_hirop {:id "1" :type :Bar :rels {:Foo "0"}}
    :title "First"}
   {:_hirop {:id "2" :type :Bar :rels {:Foo "0"}}
    :id "Second"}
   {:_hirop {:id "3" :type :Baz :rels {:Bar "2"}}
    :title "Third"}
   {:_hirop {:id "4" :type :Baz :rels {:Bar "2"}}
    :id "Fourth"}
   {:_hirop {:id "5" :type :Baz :rels {:Bar "1"}}
    :title "Fifth"}
   {:_hirop {:id "6" :type :Baz :rels {:Bar "1"}}
    :id "Sixth"}])

(deftest select-all-test
  (let [context
        (->
         (create-context :Test select-all-test-context doctypes {:Foo "0"} {} :none)
         (fetch select-all-test-fetcher)
         (merge-remote))
        context-all (select-document context "0" :test-all)
        context-first (select-document context "0" :test-first)]
    (is (= (get-in context-all [:selected :test-all])
           {:Baz ["5" "6" "3" "4"], :Bar ["1" "2"], :Foo ["0"]}))
    ;; test reselection
    (is (= (get-in (select-document context-all "0" :test-all) [:selected :test-all])
           {:Baz ["5" "6" "3" "4"], :Bar ["1" "2"], :Foo ["0"]}))
    (is (= (get-in context-first [:selected :test-first])
           {:Baz ["5" "3"], :Bar ["1" "2"], :Foo ["0"]}))))



(def select-loop-test-context
  {:relations
   [{:from :Bar :to :Foo :external true}
    {:from :Baz :to :Foo :external true}
    {:from :Baq :to :Bar}
    {:from :Baq :to :Baz}]
   :selections
   {:test
    {:Bar {:select :all}
     :Baz {:select :all}
     :Baq {:select :all}}}})

(defn select-loop-test-fetcher [_]
  [{:_hirop {:id "0" :type :Foo}
    :id "0"}
   {:_hirop {:id "1" :type :Bar :rels {:Foo "0"}}
    :title "First"}
   {:_hirop {:id "2" :type :Bar :rels {:Foo "0"}}
    :id "Second"}
   {:_hirop {:id "3" :type :Baz :rels {:Foo "0"}}
    :title "Third"}
   {:_hirop {:id "4" :type :Baz :rels {:Foo "0"}}
    :id "Fourth"}
   {:_hirop {:id "5" :type :Baq :rels {:Bar "1" :Baz "3"}}
    :title "Fifth"}
   {:_hirop {:id "6" :type :Baq :rels {:Bar "1" :Baz "3"}}
    :id "Sixth"}
   {:_hirop {:id "7" :type :Baq :rels {:Bar "1" :Baz "3"}}
    :title "Seventh"}
   {:_hirop {:id "8" :type :Baq :rels {:Bar "2" :Baz "4"}}
    :id "Eighth"}
   {:_hirop {:id "9" :type :Baq :rels {:Bar "2" :Baz "4"}}
    :title "Nineth"}
   {:_hirop {:id "10" :type :Baq :rels {:Bar "2" :Baz "4"}}
    :id "Tenth"}])

(deftest select-loop-test
  (let [context
        (->
         (create-context :Test select-loop-test-context doctypes {:Foo "0"} {} :none)
         (fetch select-loop-test-fetcher)
         (merge-remote)
         (select-document "1" :test))]
    (is (= (get-in context [:selected :test])
           {:Baz ["3"], :Baq ["5" "6" "7"] :Bar ["1"]}))))



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

(defn select-defaults-test-fetcher [_]
  [{:_hirop {:id "0" :type :Foo}
    :id "0"}])

(deftest select-defaults-test
  (let [context
        (->
         (create-context :Test select-defaults-test-context doctypes {:Foo "0"} {} :none)
         (fetch select-defaults-test-fetcher)
         (merge-remote)
         (select-defaults :test-defaults)
         (select-defaults :test-defaults))]
    (is (= {:Foo [{:_hirop {:id "0" :type :Foo} :id "0"}]}
           (checkout-selected context :test-defaults)))))


(def conflict-test-context
  {:relations []
   :selections {}})

(defn conflict-test-fetcher-1 [_]
  [{:_hirop {:id "0" :type :Foo :meta {}}
    :id "0"}])

(defn conflict-test-fetcher-2 [_]
  [{:_hirop {:id "0" :type :Foo :meta {}}
    :id "0"}
   {:_hirop {:id "1" :type :Bar :meta {} :rels {:Foo "0"}}
    :title ""}])

(defn conflict-test-fetcher-3 [_]
  [{:_hirop {:id "0" :type :Foo :meta {}}
    :id "0"}
   {:_hirop {:id "1" :type :Bar :meta {} :rels {:Foo "0"}}
    :title "BAR"}])

(defn conflict-test-saver-1 [_]
  {:result :success :remap {"tmp1" "1"}})

(defn conflict-test-saver-2 [_]
  {:result :success :remap {}})

(deftest remap-test
  (let [context
        (->
         (create-context :Test remap-test-context doctypes {:Foo "0"} {} :none)
         (fetch conflict-test-fetcher-1)
         (merge-remote))
        bar (assoc (new-document context :Bar) :_hirop {:rels {:Foo "0"}})
        context
        (->
         (commit context bar)
         (push conflict-test-saver-1)
         (fetch conflict-test-fetcher-2)
         (merge-remote))
        bar (first (checkout context :Bar))
        bar (assoc bar :title "BAR")
        context
        (->
         (commit context bar)
         (push conflict-test-saver-2)
         (fetch conflict-test-fetcher-3)
         (merge-remote))]
    (is (empty? (checkout-conflicted context)))))
