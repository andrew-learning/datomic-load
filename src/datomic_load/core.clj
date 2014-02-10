(ns datomic-load.core
  (:require [datomic.api :refer [q db] :as d]
            [clojure.pprint :refer :all]))

(def uri "datomic:free://localhost:4334//news")
(d/create-database uri)
(def conn (d/connect uri))

(def schema-tx (read-string (slurp "data/schema.dtm")))
(println "schema-tx:")
(pprint schema-tx)

@(d/transact conn schema-tx)

;; add some data:
(def data-tx [{:news/title "Rain Today", :news/url "http://test.com/news1", :db/id #db/id[:db.part/user -1000001]}])

@(d/transact conn data-tx)

(def results (q '[:find ?n :where [?n :news/title]] (db conn)))
(println (count results))
(pprint results)
(pprint (first results))

(def id (ffirst results))
(def entity (-> conn db (d/entity id)))

;; display the entity map's keys
(pprint (keys entity))

;; display the value of the entity's community name
(println (:news/title entity))
