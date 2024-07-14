<?php
/** ---------------------------------------------------------------------
 * 
 */

set_time_limit(3600);

class ExportService
{
  public static $CREATION_NONAV_TYPE_ID = "28";
  public static $CREATION_AV_TYPE_ID = "25";
  public static $ITEM_TYPE_ID = "23";
  public static $EXPORT_LIMIT = 100000;
  public static $SERVER_BASE_URL = "https://www.vhh-dev.max-recall.com/mmsi/api/ca/";
  public static function dispatchNonAv($ps_query, $vb_collect_images, $vb_collect_meta, $vb_show_json, $vb_no_zip)
  {
    ExportService::loadConfig();
    return ExportService::processNonAv($ps_query, $vb_collect_images, $vb_collect_meta, $vb_show_json, $vb_no_zip);
  }

  public static function dispatchAv($ps_query, $vb_show_json, $vb_no_zip)
  {
    ExportService::loadConfig();
    return ExportService::processAv($ps_query, $vb_show_json, $vb_no_zip);
  }

  public static function dispatchFlat($ps_object_type, $ps_query, $vb_show_json, $vb_no_zip, $vb_add_agents)
  {
    ExportService::loadConfig();
    return ExportService::processFlat($ps_object_type, $ps_query, $vb_show_json, $vb_no_zip, $vb_add_agents);
  }

  public static function loadConfig()
  {
    $jsonData = file_get_contents(dirname(__FILE__).'/ExportServiceConfig.json');

    if ($jsonData === false) {
      return;
    }

    $json = json_decode($jsonData, true);

    if (json === null) {
      return;
    }

    if ($json["creation_av_type_id"]) {
      ExportService::$CREATION_AV_TYPE_ID = $json["creation_av_type_id"];
    }

    if ($json["creation_non_av_type_id"]) {
      ExportService::$CREATION_NONAV_TYPE_ID = $json["creation_non_av_type_id"];
    }

    if ($json["item_type_id"]) {
      ExportService::$ITEM_TYPE_ID = $json["item_type_id"];
    }

    if ($json["export_limit"]) {
      ExportService::$EXPORT_LIMIT = $json["export_limit"];
    }

    if ($json["server_base_url"]) {
      ExportService::$SERVER_BASE_URL = $json["server_base_url"];
    }
  }

  private static function processNonAv($ps_query, $vb_collect_images, $vb_collect_meta, $vb_show_json, $vb_no_zip)
  {
    $startTime = microtime(true);
    $response = [];
    $uniqueId = uniqid();

    $path = __CA_BASE_DIR__ . '/media/mmsi/export/' . $uniqueId . '/';
    $imagePath = $path . 'images/';
    mkdir($path, 0775, true);

    if (!($caObjectSearch = caGetSearchInstance('ca_objects'))) {
      return array('error' => 'Could not get search instance for ca_objects');
    }

    // STEP 1: Search for all NonAVCreations and their manifestations, items and representations, entities, places, occurrences, collections

    $searchResult = $caObjectSearch->search('ca_objects.type_id:' . ExportService::$CREATION_NONAV_TYPE_ID . ' AND (' . $ps_query . ')', [
      'sort' => 'object_id',
      'sortDirection' => 'asc',
      'limit' => ExportService::$EXPORT_LIMIT
    ]);

    $rows = [];

    $caEntitiesById = [];
    $caPlacesById = [];
    $caOccurrencesById = [];
    $caCollectionsById = [];

    while ($searchResult->nextHit()) {
      $creationId = $searchResult->get('object_id');
      $creation = new ca_objects($creationId);

      $row = [
        'id' => $creationId,
        'idno' => $searchResult->get('idno'),
        'preferred_label' => $searchResult->get('ca_objects.preferred_labels.name')
      ];

      if ($vb_collect_meta) {
        $attributes = ExportService::getAllAttributes($creation, 'ca_objects');
        $entityRelations = ExportService::getRelations($creationId, 'entities', 'entity_id', 'entity_id', $caEntitiesById);
        $placeRelations = ExportService::getRelations($creationId, 'places', 'place_id', 'place_id', $caPlacesById);
        $occurrenceRelations = ExportService::getRelations($creationId, 'occurrences', 'occurrence_id', 'occurrence_id', $caOccurrencesById);
        $collectionRelations = ExportService::getRelations($creationId, 'collections', 'collection_id', 'collection_id', $caCollectionsById);

        $row['attributes'] = $attributes;
        $row['entities'] = $entityRelations;
        $row['places'] = $placeRelations;
        $row['occurrences'] = $occurrenceRelations;
        $row['collections'] = $collectionRelations;
      }

      if ($creation) {
        $subItems = ExportService::fetchRepresentationPath($creationId, $vb_collect_meta, true);
        $row['subItems'] = $subItems;
      }

      $rows[] = $row;
    }

    // STEP 2: Create Main CSV and other CSVs
    if ($vb_collect_meta && !$vb_no_zip) {
      ExportService::writeCsv($path . 'main.csv', ExportService::createMainCsvData($rows));

      // Step 3: Create other CSVs
      ExportService::writeCsv($path . 'agents.csv', ExportService::createRelCsvData($caEntitiesById, 'agent'));
      ExportService::writeCsv($path . 'places.csv', ExportService::createRelCsvData($caPlacesById, 'place'));
      ExportService::writeCsv($path . 'events.csv', ExportService::createRelCsvData($caOccurrencesById, 'event'));
      ExportService::writeCsv($path . 'collections.csv', ExportService::createRelCsvData($caCollectionsById, 'collection'));

      $zipFilename = $path . 'metadata.zip';
      $zip = new ZipArchive;

      if ($zip->open($zipFilename, ZipArchive::CREATE | ZipArchive::OVERWRITE) === TRUE) {
        $zip->addEmptyDir('metadata');
        $zip->addFile($path . 'main.csv', 'metadata/' . 'main.csv');
        $zip->addFile($path . 'agents.csv', 'metadata/' . 'agents.csv');
        $zip->addFile($path . 'places.csv', 'metadata/' . 'places.csv');
        $zip->addFile($path . 'events.csv', 'metadata/' . 'events.csv');
        $zip->addFile($path . 'collections.csv', 'metadata/' . 'collections.csv');
        $zip->close();
      }

      $response['metadata_zip'] = "/media/mmsi/export/$uniqueId/metadata.zip";
    }

    // Step 4: Collect and zip images
    if ($vb_collect_images && !$vb_no_zip) {
      mkdir($imagePath, 0775, true);

      foreach ($rows as &$row) {
        if ($row['subItems'] && !empty($row['subItems']['representations'])) {
          foreach ($row['subItems']['representations'] as &$representation) {
            $sourcePath = $representation['path'];

            if ($sourcePath && file_exists($path)) {
              $representation['exists'] = true;
              $pathInfo = pathinfo($sourcePath);

              $targetPath = $imagePath . $row['idno'];

              if (!empty($representation['item_idno'])) {
                $targetPath = $targetPath . '_' . $representation['item_idno'];
              }

              $targetPath = $targetPath = $targetPath . '_' . $representation['id'] . '.' . $pathInfo['extension'];
              $representation['targetPath'] = $targetPath;

              if (copy($sourcePath, $targetPath)) {
                $representation['copySuccess'] = true;
              } else {
                $representation['copySuccess'] = false;
              }
            }
          }
        }
      }
      unset($row);

      $zipFilename = $path . 'images.zip';
      $zip = new ZipArchive;

      if ($zip->open($zipFilename, ZipArchive::CREATE | ZipArchive::OVERWRITE) === TRUE) {
        $dir = opendir($imagePath);
        $zip->addEmptyDir('images');

        while ($file = readdir($dir)) {
          if (is_file($imagePath . $file)) {
            $zip->addFile($imagePath . $file, 'images/' . $file);
          }
        }

        $zip->close();
      }

      $response['image_zip'] = "/media/mmsi/export/$uniqueId/images.zip";

      $dir = opendir($imagePath);

      while ($file = readdir($dir)) {
        if (is_file($imagePath . $file)) {
          unlink($imagePath . $file);
        }
      }

      rmdir($imagePath);
    }

    $response['execution_time_seconds'] = microtime(true) - $startTime;
    $response['creations'] = count($rows);

    if ($vb_show_json) {
      $response['rows'] = $rows;
    }

    return [
      'response' => $response
    ];
  }

  private static function processAv($ps_query, $vb_show_json, $vb_no_zip)
  {
    // https://ca.vhh-dev.max-recall.com/providence/service.php/export/ca_objects?query=ca_objects.preferred_labels.name:(Test)&type=av&show_json=1
    $startTime = microtime(true);
    $response = [];
    $uniqueId = uniqid();

    $path = __CA_BASE_DIR__ . '/media/mmsi/export/' . $uniqueId . '/';
    mkdir($path, 0775, true);

    if (!($caObjectSearch = caGetSearchInstance('ca_objects'))) {
      return array('error' => 'Could not get search instance for ca_objects');
    }

    // STEP 1: Search for all NonAVCreations and their manifestations, items and representations, entities, places, occurrences, collections

    $searchResult = $caObjectSearch->search('ca_objects.type_id:' . ExportService::$CREATION_AV_TYPE_ID . ' AND (' . $ps_query . ')', [
      'sort' => 'object_id',
      'sortDirection' => 'asc',
      'limit' => ExportService::$EXPORT_LIMIT
    ]);

    $rows = [];

    $caEntitiesById = [];
    $caPlacesById = [];
    $caOccurrencesById = [];
    $caCollectionsById = [];

    while ($searchResult->nextHit()) {
      $creationId = $searchResult->get('object_id');
      $creation = new ca_objects($creationId);

      $row = [
        'id' => $creationId,
        'idno' => $searchResult->get('idno'),
        'preferred_label' => $searchResult->get('ca_objects.preferred_labels.name')
      ];

      $attributes = ExportService::getAllAttributes($creation, 'ca_objects');
      $entityRelations = ExportService::getRelations($creationId, 'entities', 'entity_id', 'entity_id', $caEntitiesById);
      $placeRelations = ExportService::getRelations($creationId, 'places', 'place_id', 'place_id', $caPlacesById);
      $occurrenceRelations = ExportService::getRelations($creationId, 'occurrences', 'occurrence_id', 'occurrence_id', $caOccurrencesById);
      $collectionRelations = ExportService::getRelations($creationId, 'collections', 'collection_id', 'collection_id', $caCollectionsById);

      $row['attributes'] = $attributes;
      $row['entities'] = $entityRelations;
      $row['places'] = $placeRelations;
      $row['occurrences'] = $occurrenceRelations;
      $row['collections'] = $collectionRelations;

      $subItems = ExportService::fetchRepresentationPath($creationId, true, false);
      $row['subItems'] = $subItems;

      $rows[] = $row;
    }

    if (!$vb_no_zip) {
      // STEP 2: Create Main CSV and other CSVs
      ExportService::writeCsv($path . 'main.csv', ExportService::createMainCsvData($rows));

      // Step 3: Create other CSVs
      ExportService::writeCsv($path . 'agents.csv', ExportService::createRelCsvData($caEntitiesById, 'agent'));
      ExportService::writeCsv($path . 'places.csv', ExportService::createRelCsvData($caPlacesById, 'place'));
      ExportService::writeCsv($path . 'events.csv', ExportService::createRelCsvData($caOccurrencesById, 'event'));
      ExportService::writeCsv($path . 'collections.csv', ExportService::createRelCsvData($caCollectionsById, 'collection'));

      $zipFilename = $path . 'metadata.zip';
      $zip = new ZipArchive;

      if ($zip->open($zipFilename, ZipArchive::CREATE | ZipArchive::OVERWRITE) === TRUE) {
        $zip->addEmptyDir('metadata');
        $zip->addFile($path . 'main.csv', 'metadata/' . 'main.csv');
        $zip->addFile($path . 'agents.csv', 'metadata/' . 'agents.csv');
        $zip->addFile($path . 'places.csv', 'metadata/' . 'places.csv');
        $zip->addFile($path . 'events.csv', 'metadata/' . 'events.csv');
        $zip->addFile($path . 'collections.csv', 'metadata/' . 'collections.csv');
        $zip->close();
      }

      $response['metadata_zip'] = "/media/mmsi/export/$uniqueId/metadata.zip";
    }

    $response['execution_time_seconds'] = microtime(true) - $startTime;
    $response['creations'] = count($rows);

    if ($vb_show_json) {
      $response['rows'] = $rows;
    }

    return [
      'response' => $response
    ];
  }

  private static function processFlat($objectType, $query, $showJson, $vb_no_zip, $vb_add_agents)
  {
    $startTime = microtime(true);
    $response = [];
    $uniqueId = uniqid();
    $hasRepresenationIds = false;
    $db = new Db();

    $path = __CA_BASE_DIR__ . '/media/mmsi/export/' . $uniqueId . '/';
    mkdir($path, 0775, true);

    if (!($caObjectSearch = caGetSearchInstance($objectType))) {
      return array('error' => 'Could not get search instance for ' . $objectType);
    }

    $typeMap = [
      'ca_objects' => [
        idKey => 'object_id',
        classType => ca_objects
      ],
      'ca_entities' => [
        idKey => 'entity_id',
        classType => ca_entities
      ],
      'ca_places' => [
        idKey => 'place_id',
        classType => ca_places
      ],
      'ca_occurrences' => [idKey => 'occurrence_id', classType => ca_occurrences],
      'ca_collections' => [idKey => 'collection_id', classType => ca_collections]
    ];

    $typeInfo = $typeMap[$objectType];

    $searchResult = $caObjectSearch->search($query, [
      'sort' => $typeInfo['idKey'],
      'sortDirection' => 'asc',
      'limit' => ExportService::$EXPORT_LIMIT
    ]);

    $rows = [];

    while ($searchResult->nextHit()) {
      $id = $searchResult->get($typeInfo['idKey']);

      $row = [
        'id' => $id,
        'idno' => $searchResult->get('idno'),
        'preferred_label' => $searchResult->get($objectType . '.preferred_labels.name')
      ];

      $caTableObject = new $typeInfo['classType']($id);
      $row['attributes'] = ExportService::getAllAttributes($caTableObject, $objectType);

      $subTypeId = $searchResult->get($objectType . '.type_id');
      $row['sub_type'] = $caTableObject->getTypeName($subTypeId);

      if ($subTypeId == ExportService::$ITEM_TYPE_ID) {
        $repRelSql = 'SELECT representation_id FROM ca_objects_x_object_representations ';
        $repRelSql .= 'WHERE ca_objects_x_object_representations.object_id = "' . $id . '" ';
        $repRelSql .= 'AND ca_objects_x_object_representations.is_primary = 1 ';
        $repRelSql .= 'LIMIT 1;';

        $repRelDbResult = $db->query($repRelSql);

        while ($repRelDbResult->nextRow()) {
          $dbRow = $repRelDbResult->getRow();
          $hasRepresenationIds = true;
          $row['representation_id'] = $dbRow['representation_id'];
        }
      }

      if ($vb_add_agents) {
        $row['entities'] = ExportService::getRelations($id, 'entities', 'entity_id', 'entity_id');
      }

      $rows[] = $row;
    }

    if (!$vb_no_zip) {
      $csvFileName = $objectType . '.csv';

      ExportService::writeCsv($path . $csvFileName, ExportService::createFlatCsvData($rows, $hasRepresenationIds));

      $zipFilename = $path . 'metadata.zip';
      $zip = new ZipArchive;

      if ($zip->open($zipFilename, ZipArchive::CREATE | ZipArchive::OVERWRITE) === TRUE) {
        $zip->addFile($path . $csvFileName, $csvFileName);
        $zip->close();
      }

      $response['metadata_zip'] = "/media/mmsi/export/$uniqueId/metadata.zip";
    }

    $response['execution_time_seconds'] = microtime(true) - $startTime;
    $response['count'] = count($rows);

    if ($showJson) {
      $response['data'] = $rows;
    }

    return [
      'response' => $response
    ];
  }

  private static function getTypeIdForTypeCode($typeCode)
  {
    $db = new Db();

    $sql = 'SELECT type_id FROM ca_relationship_types WHERE ca_relationship_types.type_code = "' . $typeCode . '" LIMIT 1;';
    $dbResult = $db->query($sql);

    while ($dbResult->nextRow()) {
      $row = $dbResult->getRow();
      return $row['type_id'];
    }

    return null;
  }

  private static function fetchRepresentationPath($creationId, $collectMeta, $isNonAv)
  {
    $result = [
      'other_manifestations' => [],
      'manifestations' => [],
      'items' => [],
      'representations' => []
    ];

    $db = new Db();
    $typeId = ExportService::getTypeIdForTypeCode($isNonAv ? 'IsManifestationOfNonAV' : 'IsManifestationOfAV');

    // Try to fetch AV Manifestation
    $sql = 'SELECT ca_objects_x_objects.object_left_id AS object_left_id, ca_objects_x_objects.object_right_id AS object_right_id, ca_relationship_types.type_code AS type_code ';
    $sql .= 'FROM ca_objects_x_objects ';
    $sql .= 'INNER JOIN ca_relationship_types ON ca_relationship_types.type_id = ca_objects_x_objects.type_id ';
    $sql .= 'WHERE (ca_objects_x_objects.object_left_id = "' . $creationId . '" ';
    $sql .= 'OR ca_objects_x_objects.object_right_id = "' . $creationId . '") ';
    $sql .= 'AND ca_objects_x_objects.type_id = "' . $typeId . '" '; // has AV/NonAV Manifestation
    $sql .= 'LIMIT 100;';

    $dbResult = $db->query($sql);

    while ($dbResult->nextRow()) {
      $row = $dbResult->getRow();
      $manifestationId = ($row['object_left_id'] == $creationId) ? $row['object_right_id'] : $row['object_left_id'];
      $manifestation = new ca_objects($manifestationId);

      if ($manifestation) {
        $dataObject = ExportService::getItemsAndRepresentations($manifestationId, $collectMeta, $isNonAv);

        if ($dataObject) {
          $newManifestation = [
            'id' => $manifestationId,
            'type' => $row['type_code'],
            'idno' => $manifestation->get('idno'),
            'preferred_label' => $manifestation->get('ca_objects.preferred_labels.name'),
            'attributes' => $collectMeta ? ExportService::getAllAttributes($manifestation, 'ca_objects') : null
          ];

          $result['manifestations'][] = $newManifestation;
          $result['items'] = array_merge($result['items'], $dataObject['items']);
          $result['representations'] = array_merge($result['representations'], $dataObject['representations']);
        } else {
          $result['other_manifestations'][] = [
            'id' => $manifestationId,
            'type' => $row['type_code'],
            'idno' => $manifestation->get('idno'),
            'preferred_label' => $manifestation->get('ca_objects.preferred_labels.name'),
            'attributes' => $collectMeta ? ExportService::getAllAttributes($manifestation, 'ca_objects') : null
          ];
        }
      }
    }

    return $result;
  }

  private static function getItemsAndRepresentations($manifestationId, $collectMeta, $isNonAv)
  {
    $result = [
      'items' => [],
      'representations' => []
    ];

    $representationCount = 0;

    $subTypeId = ExportService::getTypeIdForTypeCode($isNonAv ? 'IsItemOfNonAV' : 'IsItemOfAV');
    $db = new Db();

    $subSql = 'SELECT object_left_id, object_right_id, type_id FROM ca_objects_x_objects ';
    $subSql .= 'WHERE (ca_objects_x_objects.object_left_id = "' . $manifestationId . '" ';
    $subSql .= 'OR ca_objects_x_objects.object_right_id = "' . $manifestationId . '") ';
    $subSql .= 'AND ca_objects_x_objects.type_id = "' . $subTypeId . '" '; // has Item
    $subSql .= 'LIMIT 100;';

    $subDbResult = $db->query($subSql);

    while ($subDbResult->nextRow()) {
      $subRow = $subDbResult->getRow();
      $itemId = ($subRow['object_left_id'] == $manifestationId) ? $subRow['object_right_id'] : $subRow['object_left_id'];
      $item = new ca_objects($itemId);

      if ($item) {
        $repRelSql = 'SELECT representation_id FROM ca_objects_x_object_representations ';
        $repRelSql .= 'WHERE ca_objects_x_object_representations.object_id = "' . $itemId . '" ';
        $repRelSql .= 'LIMIT 100;';

        $itemHasRepresentation = false;

        $repRelDbResult = $db->query($repRelSql);

        while ($repRelDbResult->nextRow()) {
          $repRelRow = $repRelDbResult->getRow();
          $representationId = $repRelRow['representation_id'];

          $representation = new ca_object_representations($representationId);
          $mimeType = $representation->get('mimetype');

          if (!empty($mimeType) && (($isNonAv && (strpos($mimeType, 'image/') == 0 || strpos($mimeType, 'application/pdf') == 0)) || (!$isNonAv && strpos($mimeType, 'video/') == 0))) {
            $item = new ca_objects($itemId);
            $media = $representation->getRepresentations(['original']);

            if (!empty($media)) {
              $path = $media[0]['paths']['original'];

              $result['representations'][] = [
                'mimetype' => $mimeType,
                'id' => $representationId,
                'item_idno' => $item->get('idno'),
                'access' => $representation->get('access'),
                'path' => $path,
                'url' => ExportService::$SERVER_BASE_URL . str_replace('/var/www/html/providence/', '', $path)
              ];
              
              $representationCount += 1;
              $itemHasRepresentation = true;
            }
          }
        }

        if ($itemHasRepresentation) {
          $result['items'][] = [
            'id' => $itemId,
            'idno' => $item->get('idno'),
            'preferred_label' => $item->get('ca_objects.preferred_labels.name'),
            'attributes' => $collectMeta ? ExportService::getAllAttributes($item, 'ca_objects') : null
          ];
        }
      }
    }

    if ($representationCount == 0) {
      return null;
    } else {
      return $result;
    }
  }

  private static function getAllAttributes($caObject, $tableName = 'ca_objects')
  {
    $va_codes = $caObject->getApplicableElementCodes();

    $attributes = [];

    foreach ($va_codes as $vs_code) {
      if (
        $va_vals = $caObject->get(
          $tableName . '.' . $vs_code,
          array("returnWithStructure" => true, "returnAllLocales" => true, "convertCodesToDisplayText" => true)
        )
      ) {
        $va_vals_by_locale = end($va_vals);

        foreach ($va_vals_by_locale as $vn_locale_id => $va_locale_vals) {
          foreach ($va_locale_vals as $vs_val_id => $va_actual_data) {
            if (!is_array($va_actual_data)) {
              continue;
            }
            $vs_locale_code = "none";

            $t_attr = new ca_attributes($vs_val_id);
            $valueSource = (!empty($t_attr->_FIELD_VALUES) && !empty($t_attr->_FIELD_VALUES['value_source'])) ? $t_attr->_FIELD_VALUES['value_source'] : '';

            $attributes[] = array_merge(array('key' => $vs_code, 'value_source' => $valueSource), $va_actual_data);
          }
        }
      }
    }

    return $attributes;
  }

  private static function getRelations($caObjectId, $relatedType, $relIdKey, $idKey, &$cachedRelItems = null)
  {
    $db = new Db();
    $relTableName = "ca_objects_x_" . $relatedType;
    $relations = [];

    $sql = 'SELECT ' . $relTableName . '.object_id AS object_id, ' . $relTableName . '.' . $relIdKey . ' AS ' . $relIdKey . ', ca_relationship_types.type_code AS type_code ';
    $sql .= 'FROM ' . $relTableName . ' ';
    $sql .= 'INNER JOIN ca_relationship_types ON ca_relationship_types.type_id = ' . $relTableName . '.type_id ';
    $sql .= 'WHERE ' . $relTableName . '.object_id = "' . $caObjectId . '" ';
    $sql .= 'LIMIT 100;';

    $dbResult = $db->query($sql);

    while ($dbResult->nextRow()) {
      $row = $dbResult->getRow();
      $caId = $row[$relIdKey];

      if ($cachedRelItems && $cachedRelItems[$caId]) {
        $relations[] = [
          'type' => $row['type_code'],
          'item' => $cachedRelItems[$caId]
        ];
      } else {
        if ($relatedType == 'entities') {
          $caRelItem = new ca_entities($caId);
          $preferredLabel = $caRelItem->get('ca_entities.preferred_labels');
          $tableName = 'ca_entities';
        } else if ($relatedType == 'places') {
          $caRelItem = new ca_places($caId);
          $preferredLabel = $caRelItem->get('ca_places.preferred_labels');
          $tableName = 'ca_places';
        } else if ($relatedType == 'occurrences') {
          $caRelItem = new ca_occurrences($caId);
          $preferredLabel = $caRelItem->get('ca_occurrences.preferred_labels');
          $tableName = 'ca_occurrences';
        } else if ($relatedType == 'collections') {
          $caRelItem = new ca_collections($caId);
          $preferredLabel = $caRelItem->get('ca_collections.preferred_labels');
          $tableName = 'ca_collections';
        } else {
          return null;
        }

        $relAttributes = ExportService::getAllAttributes($caRelItem, $tableName);

        if ($relAttributes) {
          $relItem = [
            'id' => $caRelItem->get($idKey),
            'idno' => $caRelItem->get('idno'),
            'sub_type' => $caRelItem->getTypeName($caRelItem->get($tableName . '.type_id')),
            'preferred_label' => $preferredLabel,
            'attributes' => $relAttributes
          ];

          $relations[] = [
            'type' => $row['type_code'],
            'item' => $relItem
          ];
          
          if ($cachedRelItems) {
            $cachedRelItems[$caId] = $relItem;
          }
        }
      }
    }

    return $relations;
  }

  private static function createFlatCsvData($rows, $hasRepresenationIds)
  {
    $maxEntities = 0;

    foreach ($rows as &$row) {
      ExportService::extractAttrs($row, $attrKeys);

      if (!empty($row['entities'])) {
        $maxEntities = max($maxEntities, count($row['entities']));
      }
    }
    unset($row);

    ksort($attrKeys);

    $csvData = [];

    $header = ['idno', 'sub_type', 'preferred_label'];

    foreach ($attrKeys as $key => $subKeys) {
      foreach ($subKeys as $subKey) {
        $header[] = $key . '.' . $subKey;
      }
      $header[] = $key . '.value_source';
    }

    if ($hasRepresenationIds) {
      $header[] = 'representation_id';
    }

    if ($maxEntities > 0) {
      for ($i = 1; $i <= $maxEntities; $i++) {
        $header[] = 'entity_' . $i . '.idno';
        $header[] = 'entity_' . $i . '.type';
        $header[] = 'entity_' . $i . '.preferred_label';
      }
    }

    $csvData[] = $header;

    foreach ($rows as &$row) {
      $rowData = [];

      ExportService::addRowData($rowData, $row, $attrKeys, false, true);

      if ($hasRepresenationIds) {
        $rowData[] = empty($row['representation_id']) ? '' : $row['representation_id'];
      }

      ExportService::addRelData($rowData, $row['entities'], $maxEntities, true);

      $csvData[] = $rowData;
    }
    unset($row);

    return $csvData;
  }

  private static function createMainCsvData($rows)
  {
    $creationAttrKeys = [];
    $manifestationsAttrKeys = [];
    $otherManifestationsAttrKeys = [];
    $itemsAttrKeys = [];
    $maxRepresentations = 0;

    $maxEntities = 0;
    $maxPlaces = 0;
    $maxOccurrences = 0;
    $maxCollections = 0;

    foreach ($rows as &$row) {
      ExportService::extractAttrs($row, $creationAttrKeys);

      $maxEntities = max($maxEntities, count($row['entities']));
      $maxPlaces = max($maxPlaces, count($row['places']));
      $maxOccurrences = max($maxOccurrences, count($row['occurrences']));
      $maxCollections = max($maxCollections, count($row['collections']));

      if ($row['subItems']) {
        foreach ($row['subItems']['manifestations'] as $manifestationIndex => &$manifestation) {
          if (empty($manifestationsAttrKeys[$manifestationIndex])) {
            $manifestationsAttrKeys[$manifestationIndex] = [];
          }
          ExportService::extractAttrs($manifestation, $manifestationsAttrKeys[$manifestationIndex]);
        }
        unset($manifestation);

        foreach ($row['subItems']['items'] as $itemIndex => &$item) {
          if (empty($itemsAttrKeys[$itemIndex])) {
            $itemsAttrKeys[$itemIndex] = [];
          }
          ExportService::extractAttrs($item, $itemsAttrKeys[$itemIndex]);
        }
        unset($item);

        foreach ($row['subItems']['other_manifestations'] as $otherManifestationIndex => &$otherManifestation) {
          if (empty($otherManifestationsAttrKeys[$otherManifestationIndex])) {
            $otherManifestationsAttrKeys[$otherManifestationIndex] = [];
          }
          ExportService::extractAttrs($otherManifestation, $otherManifestationsAttrKeys[$otherManifestationIndex]);
        }
        unset($otherManifestation);

        $maxRepresentations = max($maxRepresentations, count($row['subItems']['representations']));
      }
    }
    unset($row);

    ksort($creationAttrKeys);

    foreach ($manifestationsAttrKeys as &$manifestationsAttrKeysItem) {
      ksort($manifestationsAttrKeysItem);
    }
    unset($manifestationsAttrKeysItem);

    foreach ($itemsAttrKeys as &$itemsAttrKeysItem) {
      ksort($itemsAttrKeysItem);
    }
    unset($itemsAttrKeysItem);

    foreach ($otherManifestationsAttrKeys as &$otherManifestationsAttrKeysItem) {
      ksort($otherManifestationsAttrKeysItem);
    }
    unset($otherManifestationsAttrKeysItem);

    $csvData = [];

    $header = ['creation.idno', 'creation.preferred_label'];

    // Add Header - Creation

    foreach ($creationAttrKeys as $key => $subKeys) {
      foreach ($subKeys as $subKey) {
        $header[] = 'creation.' . $key . '.' . $subKey;
      }
      $header[] = 'creation.' . $key . '.value_source';
    }

    // Add Header - Manifestations

    if (count($manifestationsAttrKeys) > 0) {
      foreach ($manifestationsAttrKeys as $index => $manifestationsAttrItem) {
        $headerIndex = $index + 1;

        $header[] = 'manifestation_' . $headerIndex . '.idno';
        $header[] = 'manifestation_' . $headerIndex . '.preferred_label';
        $header[] = 'manifestation_' . $headerIndex . '.type';

        foreach ($manifestationsAttrItem as $key => $subKeys) {
          foreach ($subKeys as $subKey) {
            $header[] = 'manifestation_' . $headerIndex . '.' . $key . '.' . $subKey;
          }
          $header[] = 'manifestation_' . $headerIndex . '.' . $key . '.value_source';
        }
      }
    }

    if (count($itemsAttrKeys) > 0) {
      foreach ($itemsAttrKeys as $index => $itemsAttrItem) {
        $headerIndex = $index + 1;

        $header[] = 'item_' . $headerIndex . '.idno';
        $header[] = 'item_' . $headerIndex . '.preferred_label';
        $header[] = 'item_' . $headerIndex . '.type';

        foreach ($itemsAttrItem as $key => $subKeys) {
          foreach ($subKeys as $subKey) {
            $header[] = 'item_' . $headerIndex . '.' . $key . '.' . $subKey;
          }
          $header[] = 'item_' . $headerIndex . '.' . $key . '.value_source';
        }
      }
    }

    // Add Header - Representations

    if ($maxRepresentations > 0) {
      for ($i = 1; $i <= $maxRepresentations; $i++) {
        $header[] = 'representation_' . $i . '.representation_id';
        $header[] = 'representation_' . $i . '.accessible_to_public';
        $header[] = 'representation_' . $i . '.item_idno';
        $header[] = 'representation_' . $i . '.url';
      }
    }

    // Add Header - Other Manifestations

    if (count($otherManifestationsAttrKeys) > 0) {
      foreach ($otherManifestationsAttrKeys as $index => $otherManifestationsAttrItem) {
        $headerIndex = $index + 1;

        $header[] = 'other_manifestation_' . $headerIndex . '.idno';
        $header[] = 'other_manifestation_' . $headerIndex . '.preferred_label';
        $header[] = 'other_manifestation_' . $headerIndex . '.type';

        foreach ($otherManifestationsAttrItem as $key => $subKeys) {
          foreach ($subKeys as $subKey) {
            $header[] = 'other_manifestation_' . $headerIndex . '.' . $key . '.' . $subKey;
          }
          $header[] = 'other_manifestation_' . $headerIndex . '.' . $key . '.value_source';
        }
      }
    }

    if ($maxEntities > 0) {
      for ($i = 1; $i <= $maxEntities; $i++) {
        $header[] = 'entity_' . $i . '.idno';
        $header[] = 'entity_' . $i . '.type';
      }
    }

    if ($maxPlaces > 0) {
      for ($i = 1; $i <= $maxPlaces; $i++) {
        $header[] = 'place_' . $i . '.idno';
        $header[] = 'place_' . $i . '.type';
      }
    }

    if ($maxOccurrences > 0) {
      for ($i = 1; $i <= $maxOccurrences; $i++) {
        $header[] = 'occurrence_' . $i . '.idno';
        $header[] = 'occurrence_' . $i . '.type';
      }
    }

    if ($maxCollections > 0) {
      for ($i = 1; $i <= $maxCollections; $i++) {
        $header[] = 'collection_' . $i . '.idno';
        $header[] = 'collection_' . $i . '.type';
      }
    }

    $csvData[] = $header;

    foreach ($rows as &$row) {
      $rowData = [];

      ExportService::addRowData($rowData, $row, $creationAttrKeys);

      // ExportService::addRowData($rowData, $row['subItems']['manifestation'], $manifestationAttrKeys, true);
      if (count($manifestationsAttrKeys) > 0) {
        foreach ($manifestationsAttrKeys as $index => $manifestationsAttrKeysItem) {
          ExportService::addRowData($rowData, $row['subItems']['manifestations'][$index], $manifestationsAttrKeysItem, true);
        }
      }

      // ExportService::addRowData($rowData, $row['subItems']['item'], $itemAttrKeys);
      if (count($itemsAttrKeys) > 0) {
        foreach ($itemsAttrKeys as $index => $itemsAttrKeysItem) {
          ExportService::addRowData($rowData, $row['subItems']['items'][$index], $itemsAttrKeysItem, true);
        }
      }

      if ($maxRepresentations > 0) {
        for ($i = 0; $i < $maxRepresentations; $i++) {
          if (empty($row['subItems']['representations'][$i])) {
            $rowData[] = '';
            $rowData[] = '';
            $rowData[] = '';
            $rowData[] = '';
          } else {
            $representation = $row['subItems']['representations'][$i];
            $rowData[] = $representation['id'];
            $rowData[] = $representation['access'];
            $rowData[] = $representation['item_idno'];
            $rowData[] = $representation['url'];
          }
        }
      }

      if (count($otherManifestationsAttrKeys) > 0) {
        foreach ($otherManifestationsAttrKeys as $index => $otherManifestationsAttrKeysItem) {
          ExportService::addRowData($rowData, $row['subItems']['other_manifestations'][$index], $otherManifestationsAttrKeysItem, true);
        }
      }

      ExportService::addRelData($rowData, $row['entities'], $maxEntities);
      ExportService::addRelData($rowData, $row['places'], $maxPlaces);
      ExportService::addRelData($rowData, $row['occurrences'], $maxOccurrences);
      ExportService::addRelData($rowData, $row['collections'], $maxCollections);

      $csvData[] = $rowData;
    }
    unset($row);

    return $csvData;
  }

  private static function addRowData(&$rowData, $object, $attrKeys, $addRelType = false, $addSubType = false)
  {
    if ($object) {
      $rowData[] = $object['idno'];

      if ($addSubType) {
        $rowData[] = $object['sub_type'];
      }

      $rowData[] = $object['preferred_label'];

      if ($addRelType) {
        $rowData[] = $object['type'];
      }

      foreach ($attrKeys as $key => $subKeys) {
        if ($object['attrsByKey'][$key]) {
          foreach ($subKeys as $subKey) {
            $rowData[] = $object['attrsByKey'][$key][$subKey];
          }
          $rowData[] = $object['attrsByKey'][$key]['value_source'];
        } else {
          foreach ($subKeys as $subKey) {
            $rowData[] = '';
          }
          $rowData[] = '';
        }
      }
    } else {
      $rowData[] = '';
      $rowData[] = '';

      if ($addRelType) {
        $rowData[] = '';
      }

      foreach ($attrKeys as $key => $subKeys) {
        foreach ($subKeys as $subKey) {
          $rowData[] = '';
        }
        $rowData[] = '';
      }
    }
  }

  private static function addRelData(&$rowData, $rels, $maxRelItems, $addLabel = false)
  {
    foreach ($rels as $rel) {
      $rowData[] = $rel['item']['idno'];
      $rowData[] = $rel['type'];

      if ($addLabel) {
        $rowData[] = $rel['item']['preferred_label'];
      }
    }

    if (count($rels) < $maxRelItems) {
      for ($i = count($rels); $i < $maxRelItems; $i++) {
        $rowData[] = '';
        $rowData[] = '';
      }
    }
  }

  private static function extractAttrs(&$object, &$attrKeys)
  {
    $attributesCounter = [];
    $attributesByKey = [];

    foreach ($object['attributes'] as $attr) {
      if ($attributesCounter[$attr['key']]) {
        $attributesCounter[$attr['key']] += 1;
      } else {
        $attributesCounter[$attr['key']] = 1;
      }

      $key = $attr['key'] . '_' . $attributesCounter[$attr['key']];
      $attributesByKey[$key] = $attr;

      if (!$attrKeys[$key]) {
        $keys = array_keys($attr);
        $keys = array_diff($keys, ['key', 'value_source']);

        $attrKeys[$key] = $keys;
      }
    }

    $object['attrsByKey'] = $attributesByKey;
  }

  private static function writeCsv($path, $array)
  {
    $file = fopen($path, "w");

    foreach ($array as $key => $row) {
      fputcsv($file, $row, ';');
    }

    fclose($file);
  }

  private static function createRelCsvData($relations, $prefix)
  {
    $relAttrKeys = [];

    foreach ($relations as &$relation) {
      ExportService::extractAttrs($relation, $relAttrKeys);
    }
    unset($relation);

    ksort($relAttrKeys);

    $csvData = [];
    $header = [$prefix . '.idno', $prefix . '.sub_type', $prefix . '.preferred_label'];

    // Add Header for related items

    foreach ($relAttrKeys as $key => $subKeys) {
      foreach ($subKeys as $subKey) {
        $header[] = $prefix . '.' . $key . '.' . $subKey;
      }
      $header[] = $prefix . '.' . $key . '.value_source';
    }

    $csvData[] = $header;

    foreach ($relations as &$row) {
      $rowData = [];
      ExportService::addRowData($rowData, $row, $relAttrKeys, false, true);
      $csvData[] = $rowData;
    }
    unset($row);

    return $csvData;
  }
}