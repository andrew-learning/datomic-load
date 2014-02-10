(ns datomic-load.core
  (:require [datomic.api :refer [q db] :as d]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [monger.core :as mg]
            [clojure.pprint :refer :all]))


(def my-uri "datomic:free://localhost:4334//tracks")

(defn create-db [uri]
  (d/create-database uri))

(defn db-conn [uri]
  (d/connect uri))

(defn load-schema [conn]
  (let [schema-tx (read-string (slurp "data/tracks-schema.dtm"))]
    (println "schema-tx:")
    (pprint schema-tx)
    @(d/transact conn schema-tx) ))

(def REQUIRED-FIELDS
  {"raw.mfcc.bs-mid" {$exists true}
   "raw.mfcc.full" {$exists true}
   "title" {$exists true $nin [nil]}
   "artist" {$exists true $nin [nil]}
   "year" {$exists true $nin [nil ""]}
   "raw.genre-description" {$exists true $nin [nil]}} )

(defn read-tracks []
  (mc/find-maps 
    "tracks"
    REQUIRED-FIELDS
    ["raw.rs-length" "title" "artist" "year", "album"]))

(defn migrate-tracks [mongo-url datomic-url]
  (mg/connect-via-uri! mongo-url)
  (let [conn (db-conn datomic-url)]
    (->>
      (read-tracks)
      (map 
        (fn [trk]
          (->> 
            (zipmap 
              [:track/title 
               :track/artist 
               :track/album 
               :track/year 
               :track/genre-text 
               :track/duration-secs]
              ((juxt :title :artist :album #(-> %1 :year Integer/parseInt)  #(-> %1 :raw :genre-description) #(-> %1 :raw :rs-length Double/valueOf (int))) trk))
            (remove (comp nil? second)))))
      ((fn [tracks] (map (fn [kv-pairs temp-id] (into {:db/id (d/tempid ":db.part/user" temp-id)} kv-pairs)) tracks (iterate dec -1))))
      ((fn [data-tx] @(d/transact conn data-tx))))))

(comment

  (migrate-tracks "mongodb://127.0.0.1/autoplay" "datomic:free://localhost:4334//tracks")

  (def conn (db-conn "datomic:free://localhost:4334//tracks"))
  (load-schema conn)
  (def results (q '[:find ?n :where [?n :track/title]] (db conn)))
  (pprint results)
  (pprint (first results))

  (def id (ffirst results))
  (def entity (-> conn db (d/entity id)))
  (pprint (keys entity))
  (println (:track/title entity))

  )

