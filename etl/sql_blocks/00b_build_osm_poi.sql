-- Ricostruisce osm_poi a partire da osm_business
BEGIN;


INSERT INTO osm_poi (osm_id, name, poi_type, tags, location)
SELECT
  ob.osm_id,
  ob.name,
  ob.category,
  ob.tags,
  ob.location
FROM osm_business ob
WHERE ob.category = 'amenity'
  AND ob.subtype IN (
    'school','kindergarten','college','university',
    'hospital','clinic','doctors','pharmacy',
    'bus_station','bus_stop','tram_stop','railway_station',
    'marketplace','theatre','cinema','stadium',
    'library','courthouse','townhall','police','post_office',
    'place_of_worship'  -- se vuoi includerlo, altrimenti rimuovi
  );

COMMIT;
