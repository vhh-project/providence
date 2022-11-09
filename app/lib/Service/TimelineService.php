<?php
/** ---------------------------------------------------------------------
 * 
*/

require_once(__CA_MODELS_DIR__."/ca_object_representations.php");

class TimelineService {
	public static function dispatch($ps_timeline_type, $force_refresh = false) {
		$path = __CA_BASE_DIR__.'/media/mmsi/timeline/'.$ps_timeline_type.'.json';

		switch ($ps_timeline_type) {
			case 'tba_dates':
				return TimelineService::getTBADates($force_refresh, $path);
				break;

			case 'av_creations':
				return TimelineService::getAVCreations($force_refresh, $path);
				break;
			
			case 'nonav_creations':
				return TimelineService::getNonAVCreations($force_refresh, $path);
				break;

			case 'events':
				return TimelineService::getHistoricEvents($force_refresh, $path);
				break;

			default:
				return array('error' => 'Wrong timeline data type: '.$ps_timeline_type);
		}
	}

	private static function fetchTBAs($query) {
		$esQuery = [
			'sort' => [
				'videoId' => 'asc',
				'inPoint' => 'asc'
			],
			'size' => 100000,
			'from' => 0,
			'query' => $query
		];
	
		$url = 'tba-elasticsearch:9200/tba/_search';
		
		$ch = curl_init();
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HEADER, false);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_HTTPHEADER, [
			'Content-Type: application/json'
		]);
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($esQuery));
    
		$esResponse = json_decode(curl_exec($ch), true);
    
		curl_close($ch);

		return $esResponse;
	}

	private static function fetchRelTBAs($query) {
		$esQuery = [
			'sort' => [
				'leftValue' => 'asc',
				'leftStart' => 'asc'
			],
			'size' => 200,
			'from' => 0,
			'query' => $query
		];
	
		$url = 'tba-elasticsearch:9200/rel/_search';
		
		$ch = curl_init();
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HEADER, false);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_HTTPHEADER, [
			'Content-Type: application/json'
		]);
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_POST, true);
    curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($esQuery));
    
		$esResponse = json_decode(curl_exec($ch), true);
    
		curl_close($ch);

		return $esResponse;
	}

	private static function fetchCaObjectByVideoId($videoId, $fetchCreation = false) {
		$objectId = null;
		$db = new Db();

		$sql = 'SELECT object_id FROM ca_objects_x_object_representations ';
		$sql .= 'WHERE ca_objects_x_object_representations.representation_id = "'.$videoId.'" '; 
		$sql .= 'AND ca_objects_x_object_representations.is_primary = "1" ';
		$sql .= 'LIMIT 1;';
		
		$dbResult = $db->query($sql);
		
		while($dbResult->nextRow()) {
			$row = $dbResult->getRow();
			$objectId = $row['object_id'];
		}

		if (!$objectId) {
			return null;
		}

		return TimelineService::fetchCaObjectItem($objectId, $fetchCreation);
	}

	private static function fetchCaObjectItem($objectId, $fetchCreation = false) {
		$caObject = new ca_objects($objectId);
		if (!$caObject) {
			return null;
		}

		$typeCode = $caObject->getTypeCode();

		$idno = $caObject->get('idno');
		$name = $caObject->get('ca_objects.preferred_labels.name');
		$label = (!empty($name)) ? $name : $idno;
	
		$result = [
			'id' => $caObject->get('object_id'),
			'idno' => $idno,
			'type' => $typeCode,
			'name' => $name,
			'label' => $label
		];

		if (!$fetchCreation || $typeCode != 'item') {
			return $result;
		}

		// Fetch Creation Object
		$manifestationRow = null;
		$db = new Db();
		
		// Try to fetch AV Manifestation
		$sql = 'SELECT * FROM ca_objects_x_objects ';
		$sql .= 'WHERE (ca_objects_x_objects.object_left_id = "'.$result['id'].'" '; 
		$sql .= 'OR ca_objects_x_objects.object_right_id = "'.$result['id'].'") '; 
		$sql .= 'AND ca_objects_x_objects.type_id = "368" '; // AV Manifestation
		$sql .= 'LIMIT 1;';

		$dbResult = $db->query($sql);
		while($dbResult->nextRow()) {
			$manifestationRow = $dbResult->getRow();
		}

		if (!$manifestationRow) {
			return $result;
		}
		
		if ($manifestationRow['object_left_id'] == $result['id']) {
			$manifestationId = $manifestationRow['object_right_id'];
		} else {
			$manifestationId = $manifestationRow['object_left_id'];
		}

		$caManifestation = new ca_objects($manifestationId);
		
		if ($caManifestation) {
			$idno = $caManifestation->get('idno');
			$name = $caManifestation->get('ca_objects.preferred_labels.name');
			$label = (!empty($name)) ? $name : $idno;

			$result['manifestation'] = [
				'id' => $caManifestation->get('object_id'),
				'idno' => $idno,
				'name' => $name,
				'label' => $label
			];
		}

		// Try to fetch AV Creation
		$creationRow = null;
		
		$sql = 'SELECT * FROM ca_objects_x_objects ';
		$sql .= 'WHERE (ca_objects_x_objects.object_left_id = "'.$manifestationId.'" '; 
		$sql .= 'OR ca_objects_x_objects.object_right_id = "'.$manifestationId.'") '; 
		$sql .= 'AND ca_objects_x_objects.type_id = "370" '; // AV Creation
		$sql .= 'LIMIT 1;';

		$dbResult = $db->query($sql);
		while($dbResult->nextRow()) {
			$creationRow = $dbResult->getRow();
		}

		if (!$creationRow) {
			return $result;
		}

		if ($creationRow['object_left_id'] == $manifestationId) {
			$creationId = $creationRow['object_right_id'];
		} else {
			$creationId = $creationRow['object_left_id'];
		}

		$caCreation = new ca_objects($creationId);

		if ($caCreation) {
			$idno = $caCreation->get('idno');
			$name = $caCreation->get('ca_objects.preferred_labels.name');
			$label = (!empty($name)) ? $name : $idno;

			$result['creation'] = [
				'id' => $caCreation->get('object_id'),
				'idno' => $idno,
				'name' => $name,
				'label' => $label
			];
		}

		return $result;
	}

	private static function fetchItemWithRepresentationForCreation($creationId, $creationType) {
		$db = new Db();
		
		// TODO: nonav type id
		$typeId = ($creationType == 'av') ? '370' : '371';
		$subTypeId = ($creationType == 'av') ? '368' : '369';
		
		// Try to fetch AV Manifestation
		$sql = 'SELECT object_left_id, object_right_id FROM ca_objects_x_objects ';
		$sql .= 'WHERE (ca_objects_x_objects.object_left_id = "'.$creationId.'" '; 
		$sql .= 'OR ca_objects_x_objects.object_right_id = "'.$creationId.'") '; 
		$sql .= 'AND ca_objects_x_objects.type_id = "'.$typeId.'" '; // has AV Manifestation
		$sql .= 'LIMIT 100;';

		$dbResult = $db->query($sql);

		while($dbResult->nextRow()) {
			$row = $dbResult->getRow();
			$manifestationId = ($row['object_left_id'] == $creationId) ? $row['object_right_id'] : $row['object_left_id'];

			$subSql = 'SELECT object_left_id, object_right_id, type_id FROM ca_objects_x_objects ';
			$subSql .= 'WHERE (ca_objects_x_objects.object_left_id = "'.$manifestationId.'" '; 
			$subSql .= 'OR ca_objects_x_objects.object_right_id = "'.$manifestationId.'") '; 
			$subSql .= 'AND ca_objects_x_objects.type_id = "'.$subTypeId.'" '; // has Item
			$subSql .= 'LIMIT 100;';

			$subDbResult = $db->query($subSql);
			
			while($subDbResult->nextRow()) {
				$subRow = $subDbResult->getRow();
				$itemId = ($subRow['object_left_id'] == $manifestationId) ? $subRow['object_right_id'] : $subRow['object_left_id'];
				
				$repSql = 'SELECT representation_id FROM ca_objects_x_object_representations ';
				$repSql .= 'WHERE ca_objects_x_object_representations.object_id = "'.$itemId.'" '; 
				$repSql .= 'AND ca_objects_x_object_representations.is_primary = "1" ';
				$repSql .= 'LIMIT 1;';

				$repDbResult = $db->query($repSql);
				
				while($repDbResult->nextRow()) {
					$repRow = $repDbResult->getRow();
					$representationId = $repRow['representation_id'];
					
					$representation = new ca_object_representations($representationId);
					$mimeType = $representation->get('mimetype');

					if ((($creationType == 'av') and (strpos($mimeType, 'video/') == 0)) || (strpos($mimeType, 'image/') == 0)) {
						$item = new ca_objects($itemId);
						$media = $representation->getRepresentations(['original']);

						if (!empty($media)) {
							$url = $media[0]['urls']['original'];
						}
						
						if ($item) {
							$idno = $item->get('idno');
							$name = $item->get('ca_objects.preferred_labels.name');
							$label = (!empty($name)) ? $name : $idno;

							return [
								'id' => $itemId,
								'idno' => $idno,
								'name' => $name,
								'label' => $label,
								'representation' => [
									'id' => $representationId,
									'mimetype' => $mimeType,
									'url' => $url
								]
							];
						}
					}
				}
			}
		}

		return null;
	}

	private static function findAttrByTestValue($caSearchObject, $attrName, $testIndex, $testValue, $resultIndex) {
		$array = $caSearchObject->get($attrName, [
			'returnAsArray' => true
		]);

		if (empty($array)) {
			return null;
		}
		
		foreach($array as $item) {
			$item = explode(';', $item);

			if ($item[$testIndex] == $testValue) {
				return $item[$resultIndex];
			}
		}

		return null;
	}

  private static function getTBADates($force_refresh, $file_path) {
		if (!$force_refresh) {
			if (!file_exists($file_path)) {
				$force_refresh = true;
			} else {
				$response = json_decode(file_get_contents($file_path));
				return ['response' => $response];
			}
		}
		
		// STEP 1: fetch all TBAs from the elastic 'tba' index
		$query = [
			'bool' => [
				'must' => [
					[
						'terms' => [
							'type' => [ 'date_range' ]
						]
			 		],
					[
						'bool' => [
							'minimum_should_match' => 1,
							'should' => [
								[
									'wildcard' => [
										'value' => [
											'value' => 'captured'
										]
									]
								],
							  [
							    'wildcard' => [
							      'value' => [
											'value' => 'captured_represented'
										]
							    ]
							  ],
							  [
							    'wildcard' => [
							      'value' => [
											'value' => 'represented'
										]
							    ]
							  ]
							]
						]
					],
					[
						'term' => [
							'isPublic' => true
						]
					],
					[
						'term' => [
							'isPublished' => true
						]
					]
			 	]
			]
		];

		$esResponse = TimelineService::fetchTBAs($query);

		// STEP 2: build basic response structure
		if (empty($esResponse['hits'])) {
			return array('error' => 'No hits for date_range TBAs');
		}

		$dateTBAs = [];
		$geoTBAsByVideoId = [];
		$placeTBAsByVideoId = [];
		$caPlacesByPlaceId = [];
		$caObjectsByVideoId = [];

		foreach($esResponse['hits']['hits'] as $hit) {
			if (!empty($hit['_source']['valueDateRange'])) {
				$dateFrom = substr($hit['_source']['valueDateRange']['gte'], 0, 10);
				$dateTo = substr($hit['_source']['valueDateRange']['lte'], 0, 10);
			}
			else {
				$dateFrom = null;
				$dateTo = null;
			}

			$videoId = $hit['_source']['videoId'];

			$dateTBAs []= [
				'id' => $hit['_id'],
				'videoId' => $videoId,
				'inPoint' => $hit['_source']['inPoint'],
				'outPoint' => $hit['_source']['outPoint'],
				'label' => $hit['_source']['label'],
				'value' => $hit['_source']['value'],
				'dateFrom' => $dateFrom,
				'dateTo' => $dateTo,
				'createdByUserId' => $hit['_source']['createdByUserId'],
				'createdByUserName' => $hit['_source']['createdByUserName'],
				'createdAt' => $hit['_source']['createdAt'],
				'lastModifiedByUserId' => $hit['_source']['createdByUserId'],
				'lastModifiedByUserName' => $hit['_source']['createdByUserName'],
				'lastModifiedAt' => $hit['_source']['createdAt']
			];

			$geoTBAsByVideoId[$videoId] = [];
			$placeTBAsByVideoId[$videoId] = [];
			$caObjectsByVideoId[$videoId] = [];
		}

		// STEP 3: get associated TBAs for geolocation
		// STEP 3.1: Geolocation TBAs
		foreach($geoTBAsByVideoId as $videoId => &$geoTBAs) {			
			$query = [
				'bool' => [
					'must' => [
						[
							'terms' => [
								'type' => [ 'geo_camera_viewpoint', 'geo_generic' ]
							]
				 		],
						[
							'terms' => [
								'videoId' => [ $videoId ]
							]
				 		],
						[
							'term' => [
								'isPublic' => true
							]
						],
						[
							'term' => [
								'isPublished' => true
							]
						]
					]
				]
			];

			$esResponse = TimelineService::fetchTBAs($query);
			
			if (!empty($esResponse['hits']) && ($esResponse['hits']['total']['value'] > 0)) {
				foreach($esResponse['hits']['hits'] as $hit) {
					$geoTBAs []= [
						'id' => $hit['_id'],
						'videoId' => $videoId,
						'inPoint' => $hit['_source']['inPoint'],
						'outPoint' => $hit['_source']['outPoint'],
						'label' => $hit['_source']['label'],
						'value' => $hit['_source']['value'],
						'type' => $hit['_source']['type'],
						'used' => false,
						'createdByUserId' => $hit['_source']['createdByUserId'],
						'createdByUserName' => $hit['_source']['createdByUserName'],
						'createdAt' => $hit['_source']['createdAt'],
						'lastModifiedByUserId' => $hit['_source']['createdByUserId'],
						'lastModifiedByUserName' => $hit['_source']['createdByUserName'],
						'lastModifiedAt' => $hit['_source']['createdAt']
					];
				}
			} else {
				unset($geoTBAsByVideoId[$videoId]);
			}
		}
		
		// Place relations
		foreach($placeTBAsByVideoId as $videoId => &$placeTBAs) {			
			$query = [
				'bool' => [
					'must' => [
						[
							'bool' => [
								'minimum_should_match' => 1,
								'should' => [
									[
										'term' => [
											'leftValue' => $videoId
										]
									],
									[
										'wildcard' => [
											'leftValue' => $videoId.'\:*'
										]
									]
								]
							]
						],
						[
							'term' => [
								'rightType' => 'ca_place'
							]
						],
						[
							'term' => [
								'isPublic' => true
							]
						],
						[
							'term' => [
								'isPublished' => true
							]
						]
					]
				]
			];

			$esResponse = TimelineService::fetchRelTBAs($query);

			if (!empty($esResponse['hits']) && ($esResponse['hits']['total']['value'] > 0)) {
				foreach($esResponse['hits']['hits'] as $hit) {
					$placeId =$hit['_source']['rightValue'];
					$caPlacesByPlaceId[$placeId] = [];

					$placeTBAs []= [
						'id' => $hit['_id'],
						'videoId' => $videoId,
						'inPoint' => $hit['_source']['leftStart'],
						'outPoint' => $hit['_source']['leftEnd'],
						'label' => $hit['_source']['rightLabel'],
						'placeId' => $placeId,
						'used' => false,
						'createdByUserId' => $hit['_source']['createdByUserId'],
						'createdByUserName' => $hit['_source']['createdByUserName'],
						'createdAt' => $hit['_source']['createdAt'],
						'lastModifiedByUserId' => $hit['_source']['createdByUserId'],
						'lastModifiedByUserName' => $hit['_source']['createdByUserName'],
						'lastModifiedAt' => $hit['_source']['createdAt']
					];
				}
			} else {
				unset($placeTBAsByVideoId[$videoId]);
			}
		}
		
		// STEP 4: Collecticve Access Objects according to TBAs
		foreach($caObjectsByVideoId as $videoId => $caObject) {
			$caObjectsByVideoId[$videoId] = TimelineService::fetchCaObjectByVideoId($videoId, true);
		}

		// STEP 5: Add geolocation and placeIDs
		foreach($dateTBAs as &$dateTBA) {
			$videoId = $dateTBA['videoId'];
			$inPoint = $dateTBA['inPoint'];
			$outPoint = $dateTBA['outPoint'];
			$geoTBAIds = [];
			$placeTBAIds = [];

			$geoTBAs = &$geoTBAsByVideoId[$videoId];
			if (!empty($geoTBAs)) {
				foreach($geoTBAs as &$geoTBA) {
					if ($geoTBA['inPoint'] <= $outPoint && $geoTBA['outPoint'] >= $inPoint) {
						$geoTBA['used'] = true;
						$geoTBAIds []= $geoTBA['id'];
					}
				}
			}
			$dateTBA['geoTbaIds'] = $geoTBAIds;
			
			$placeTBAs = &$placeTBAsByVideoId[$videoId];
			if (!empty($placeTBAs)) {
				foreach($placeTBAs as &$placeTBA) {
					if ($placeTBA['inPoint'] <= $outPoint && $placeTBA['outPoint'] >= $inPoint) {
						$placeTBA['used'] = true;
						$placeTBAIds []= $placeTBA['id'];
					}
				}
			}
			$dateTBA['placeTbaIds'] = $placeTBAIds;	
		}

		// STEP 6: Create a final list of geoTBAs
		$finalGeoTBAs = [];

		foreach($geoTBAsByVideoId as $videoId => &$geoTBAGroup) {
			foreach($geoTBAGroup as &$geoTBAItem) {
				if ($geoTBAItem['used']) {
					unset($geoTBAItem['used']);
					$finalGeoTBAs []= $geoTBAItem;
				}
			}
		}

		// STEP 7: Find all places with geolocations
		foreach ($caPlacesByPlaceId as $placeId => &$placeItem) {
			$caPlace = new ca_places($placeId);

			if ($caPlace && $georeference = $caPlace->get('ca_places.georeference')) {
				$placeItem['id'] = $caPlace->get('place_id');
				$placeItem['name'] = $caPlace->get('ca_places.preferred_labels.name');
				$placeItem['georeference'] = $georeference;
			} else {
				unset($caPlacesByPlaceId[$placeId]);
			}
		}

		// STEP 8: Create a final list of place TBAs
		$finalPlaceTBAs = [];

		foreach($placeTBAsByVideoId as $videoId => &$placeTBAGroup) {
			foreach($placeTBAGroup as &$placeTBAItem) {
				if ($placeTBAItem['used']) {
					$caPlace = $caPlacesByPlaceId[$placeTBAItem['placeId']];
					if ($caPlace) {
						unset($placeTBAItem['used']);
						$finalPlaceTBAs []= $placeTBAItem;
					}
				}
			}
		}

		// STEP 9: Add meaningful labels to each date TBA
		foreach($dateTBAs as &$dateTBA) {
			$item = $caObjectsByVideoId[$dateTBA['videoId']];

			if (!empty($item)) {
				if (!empty($item['creation'])) {
					$dateTBA['label'] = $item['creation']['label'];
				} else if (!empty($item['manifestation'])) {
					$dateTBA['label'] = $item['manifestation']['label'];
				} else {
					$dateTBA['label'] = $item['label'];
				}
			}
		}

		$response = [
			'dateTbas' => $dateTBAs,
			'geoTbas' => $finalGeoTBAs,
			'objectsByVideoId' => $caObjectsByVideoId,
			'placesByPlaceId' => $caPlacesByPlaceId,
			'placeTbas' => $finalPlaceTBAs
		];

		file_put_contents($file_path, json_encode($response));

		return [ 'response' => $response ];
	}

	private static function getAVCreations($force_refresh, $file_path) {
		if (!($caObjectSearch = caGetSearchInstance('ca_objects'))) {
			return array('error' => 'Could not get search instance for ca_objects');
		}

		if (!$force_refresh) {
			if (!file_exists($file_path)) {
				$force_refresh = true;
			} else {
				$response = json_decode(file_get_contents($file_path));
				return ['response' => $response];
			}
		}

		if (!$force_refresh) {
			if (!file_exists($file_path)) {
				$force_refresh = true;
			} else {
				$response = json_decode(file_get_contents($file_path));
				return ['response' => $response];
			}
		}

		// STEP 1: Fetch all relevant AV Creations and create a list

		$caAVCreations = [];
		$caPlacesByPlaceId = [];
		$geoTBAs = [];
		$queryString = 'ca_objects.type_id:25 AND ca_objects.vhh_date.date_Type:2038';

		$searchResult = $caObjectSearch->search($queryString, [
			'sort' => 'object_id',
			'sortDirection' => 'asc',
			'limit' => 10000
		]);
		
		while($searchResult->nextHit()) {
			$dateOfProduction = TimelineService::findAttrByTestValue($searchResult, 'ca_objects.vhh_date', 1, '2038', 0);
			$referenceCountryId = TimelineService::findAttrByTestValue($searchResult, 'ca_objects.vhh_CountryOfReference', 1, 'production', 0);

			if (!empty($referenceCountryId)) {
				$caPlacesByPlaceId[$referenceCountryId] = [];
			}

			$idno = $searchResult->get('idno');
			$name = $searchResult->get('ca_objects.preferred_labels.name');
			$label = (!empty($name)) ? $name : $idno;

			$caAVCreations []= [
				'id' => $searchResult->get('object_id'),
				'idno' => $idno,
				'name' => $name,
				'label' => $label,
				'dateOfProduction' => $dateOfProduction,
				'referenceCountryId' => $referenceCountryId
			];
		}

		// STEP 2: Get all place ids and the related videoIds
		$db = new Db();

		foreach($caAVCreations as &$creation) {
			// Get related Places
			$places = [];
		
			$sql = 'SELECT r.place_id, r.type_id, l.typename FROM ca_objects_x_places AS r ';
			$sql .= 'INNER JOIN ca_relationship_type_labels AS l ON l.type_id = r.type_id AND l.locale_id = 1 ';
			$sql .= 'WHERE r.object_id = "'.$creation['id'].'" ';
			$sql .= 'LIMIT 10;';
			
			$dbResult = $db->query($sql);
			
			while($dbResult->nextRow()) {
				$row = $dbResult->getRow();

				$caPlacesByPlaceId[$row['place_id']] = [];
				$places []= [
					'id' => $row['place_id'],
					'relationType' => $row['typename']
				];
			}
			
			if (!empty($places)) {
				$creation['places'] = $places;
			}

			// Get related videoId / representation_id
			$item = TimelineService::fetchItemWithRepresentationForCreation($creation['id'], 'av');
			
			if ($item) {
				$creation['item'] = $item;
			}
		}

		// STEP 3: Add Place TBAs
		foreach ($caAVCreations as &$creation) {
			$videoId = $creation['videoId'];
			
			if ($videoId) {
				$query = [
					'bool' => [
						'must' => [
							[
								'bool' => [
									'minimum_should_match' => 1,
									'should' => [
										[
											'term' => [
												'leftValue' => $videoId
											]
										],
										[
											'wildcard' => [
												'leftValue' => $videoId.'\:*'
											]
										]
									]
								]
							],
							[
								'term' => [
									'rightType' => 'ca_place'
								]
							],
							[
								'term' => [
									'isPublic' => true
								]
							],
							[
								'term' => [
									'isPublished' => true
								]
							]
						]
					]
				];

				$esResponse = TimelineService::fetchRelTBAs($query);

				if (!empty($esResponse['hits']) && ($esResponse['hits']['total']['value'] > 0)) {
					$creation['placeTBAs'] = [];

					foreach($esResponse['hits']['hits'] as $hit) {
						$placeId = $hit['_source']['rightValue'];
						$caPlacesByPlaceId[$placeId] = [];

						$creation['placeTBAs'] []= [
							'id' => $hit['_id'],
							'videoId' => $videoId,
							'inPoint' => $hit['_source']['leftStart'],
							'outPoint' => $hit['_source']['leftEnd'],
							'label' => $hit['_source']['rightLabel'],
							'placeId' => $placeId,
							'createdByUserId' => $hit['_source']['createdByUserId'],
							'createdByUserName' => $hit['_source']['createdByUserName'],
							'createdAt' => $hit['_source']['createdAt'],
							'lastModifiedByUserId' => $hit['_source']['createdByUserId'],
							'lastModifiedByUserName' => $hit['_source']['createdByUserName'],
							'lastModifiedAt' => $hit['_source']['createdAt']
						];
					}
				}
			}
		}

		// STEP 4: Resolve all related places
		foreach($caPlacesByPlaceId as $placeId => &$placeItem) {
			$caPlace = new ca_places($placeId);

			if ($caPlace && $georeference = $caPlace->get('ca_places.georeference')) {
				$placeItem['id'] = $caPlace->get('place_id');
				$placeItem['name'] = $caPlace->get('ca_places.preferred_labels.name');
				$placeItem['georeference'] = $georeference;
			} else {
				unset($caPlacesByPlaceId[$placeId]);
			}
		}

		// STEP 5: Add geo TBAs
		foreach ($caAVCreations as &$creation) {
			if ($creation['item'] && $creation['item']['representation']) {
				$videoId = $creation['item']['representation']['id'];
			
				if ($videoId) {
					$query = [
						'bool' => [
							'must' => [
								[
									'terms' => [
										'type' => [ 'geo_camera_viewpoint', 'geo_generic' ]
									]
						 		],
								[
									'terms' => [
										'videoId' => [ $videoId ]
									]
						 		],
								[
									'term' => [
										'isPublic' => true
									]
								],
								[
									'term' => [
										'isPublished' => true
									]
								]
							]
						]
					];

					$esResponse = TimelineService::fetchTBAs($query);
						
					if (!empty($esResponse['hits']) && ($esResponse['hits']['total']['value'] > 0)) {
						$creation['geoTBAIds'] = [];

						foreach($esResponse['hits']['hits'] as $hit) {
							$creation['geoTBAIds'] []= $hit['_id'];
							$geoTBAs []= [
								'id' => $hit['_id'],
								'videoId' => $videoId,
								'inPoint' => $hit['_source']['inPoint'],
								'outPoint' => $hit['_source']['outPoint'],
								'label' => $creation['name'],
								'value' => $hit['_source']['value'],
								'type' => $hit['_source']['type'],
								'createdByUserId' => $hit['_source']['createdByUserId'],
								'createdByUserName' => $hit['_source']['createdByUserName'],
								'createdAt' => $hit['_source']['createdAt'],
								'lastModifiedByUserId' => $hit['_source']['createdByUserId'],
								'lastModifiedByUserName' => $hit['_source']['createdByUserName'],
								'lastModifiedAt' => $hit['_source']['createdAt']
							];
						}
					}
				}
			}
		}

		$response = [
			'avCreations' => $caAVCreations,
			'placesByPlaceId' => $caPlacesByPlaceId,
			'geoTbas' => $geoTBAs
		];

		file_put_contents($file_path, json_encode($response));

		return [ 'response' => $response ];
	}

	private static function getNonAVCreations($force_refresh, $file_path) {
		if (!($caObjectSearch = caGetSearchInstance('ca_objects'))) {
			return array('error' => 'Could not get search instance for ca_objects');
		}

		if (!$force_refresh) {
			if (!file_exists($file_path)) {
				$force_refresh = true;
			} else {
				$response = json_decode(file_get_contents($file_path));
				return ['response' => $response];
			}
		}

		// STEP 1: Fetch all relevant NonAV Creations and create a list

		$caNonAVCreations = [];
		$caPlacesByPlaceId = [];
		$queryString = 'ca_objects.type_id:28 AND ca_objects.vhh_date.date_Type:2038';

		$searchResult = $caObjectSearch->search($queryString, [
			'sort' => 'object_id',
			'sortDirection' => 'asc',
			'limit' => 10000
		]);

		while($searchResult->nextHit()) {
			$dateOfProduction = TimelineService::findAttrByTestValue($searchResult, 'ca_objects.vhh_date', 1, '2038', 0);
			
			$viewpoints = $searchResult->get('ca_objects.vhh_Viewpoint', [
				'returnAsArray' => true
			]);

			$targetpoints = $searchResult->get('ca_objects.vhh_Targetpoint', [
				'returnAsArray' => true
			]);

			$idno = $searchResult->get('idno');
			$name = $searchResult->get('ca_objects.preferred_labels.name');
			$label = (!empty($name)) ? $name : $idno;

			$caNonAVCreation = [
				'id' => $searchResult->get('object_id'),
				'idno' => $idno,
				'name' => $name,
				'label' => $label,
				'dateOfProduction' => $dateOfProduction
			];

			if (count($viewpoints) > 0) {
				$caNonAVCreation['viewpoint'] = $viewpoints[0];
			}

			if (count($targetpoints) > 0) {
				$caNonAVCreation['targetpoint'] = $targetpoints[0];
			}

			$caNonAVCreations []= $caNonAVCreation;
		}

		// // STEP 2: Get all place ids

		$db = new Db();

		foreach($caNonAVCreations as &$creation) {
			// Get related Places
			$places = [];
		
			$sql = 'SELECT r.place_id, r.type_id, l.typename FROM ca_objects_x_places AS r ';
			$sql .= 'INNER JOIN ca_relationship_type_labels AS l ON l.type_id = r.type_id AND l.locale_id = 1 ';
			$sql .= 'WHERE r.object_id = "'.$creation['id'].'" ';
			$sql .= 'LIMIT 10;';
			
			$dbResult = $db->query($sql);
			
			while($dbResult->nextRow()) {
				$row = $dbResult->getRow();

				$caPlacesByPlaceId[$row['place_id']] = [];
				$places []= [
					'id' => $row['place_id'],
					'relationType' => $row['typename']
				];
			}
			
			if (!empty($places)) {
				$creation['places'] = $places;
			}
		}

		// STEP 3: Resolve all related places
		foreach($caPlacesByPlaceId as $placeId => &$placeItem) {
			$caPlace = new ca_places($placeId);

			if ($caPlace && $georeference = $caPlace->get('ca_places.georeference')) {
				$placeItem['id'] = $caPlace->get('place_id');
				$placeItem['name'] = $caPlace->get('ca_places.preferred_labels.name');
				$placeItem['georeference'] = $georeference;
			} else {
				unset($caPlacesByPlaceId[$placeId]);
			}
		}
		
		// This costs too much time at the moment and so far
		// STEP 4: Find related items with representation
		// foreach($caNonAVCreations as &$creation) {
		// 	$item = TimelineService::fetchItemWithRepresentationForCreation($creation['id'], 'nonav');
			
		// 	if ($item) {
		// 		$creation['item'] = $item;
		// 	}
		// }

		$response = [
			'nonAvCreations' => $caNonAVCreations,
			'placesByPlaceId' => $caPlacesByPlaceId
		];

		file_put_contents($file_path, json_encode($response));

		return [ 'response' => $response ];
	}

	private static function getHistoricEvents($force_refresh, $file_path) {
		if (!($caOccurenceSearch = caGetSearchInstance('ca_occurrences'))) {
			return array('error' => 'Could not get search instance for ca_occurrences');
		}

		if (!$force_refresh) {
			if (!file_exists($file_path)) {
				$force_refresh = true;
			} else {
				$response = json_decode(file_get_contents($file_path));
				return ['response' => $response];
			}
		}
		
		$caOccurrences = [];
		$caPlacesByPlaceId = [];

		// STEP 1: Fetch all relevant Historic Events and create a list
		$queryString = 'ca_occurrences.type_id:116';

		$searchResult = $caOccurenceSearch->search($queryString, [
			'sort' => 'occurrence_id',
			'sortDirection' => 'asc',
			'limit' => 10000
		]);
		
		while($searchResult->nextHit()) {
			$dateEvent = $searchResult->get('ca_occurrences.vhh_DateEvent');
			
			if (!empty($dateEvent)) {
				$idno = $searchResult->get('idno');
				$name = $searchResult->get('ca_occurrences.preferred_labels.name');
				$label = (!empty($name)) ? $name : $idno;

				$caOccurrences []= [
					'id' => $searchResult->get('occurrence_id'),
					'idno' => $idno,
					'name' => $name,
					'label' => $label,
					'date' => $dateEvent
				];
			}
		}

		// STEP 2: Add place relations to each event
		$db = new Db();
		
		foreach($caOccurrences as &$occurrence) {
			// Get related Places
			$places = [];
		
			$sql = 'SELECT r.place_id, r.type_id, l.typename FROM ca_places_x_occurrences AS r ';
			$sql .= 'INNER JOIN ca_relationship_type_labels AS l ON l.type_id = r.type_id AND l.locale_id = 1 ';
			$sql .= 'WHERE r.occurrence_id = "'.$occurrence['id'].'" ';
			$sql .= 'LIMIT 10;';
			
			$dbResult = $db->query($sql);
			
			while($dbResult->nextRow()) {
				$row = $dbResult->getRow();

				$caPlacesByPlaceId[$row['place_id']] = [];
				$places []= [
					'id' => $row['place_id'],
					'relationType' => $row['typename']
				];
			}
			
			if (!empty($places)) {
				$occurrence['places'] = $places;
			}
		}

		// STEP 3: Find all places with geolocations
		foreach ($caPlacesByPlaceId as $placeId => &$placeItem) {
			$caPlace = new ca_places($placeId);

			if ($caPlace && $georeference = $caPlace->get('ca_places.georeference')) {
				$placeItem['id'] = $caPlace->get('place_id');
				$placeItem['name'] = $caPlace->get('ca_places.preferred_labels.name');
				$placeItem['georeference'] = $georeference;
			} else {
				unset($caPlacesByPlaceId[$placeId]);
			}
		}

		// STEP 4: Find related av_creations and non_av_creations
		foreach($caOccurrences as &$occurrence) {
			$avCreationIds = [];
			$nonAvCreationIds = [];

			$sql = 'SELECT r.object_id, r.type_id, l.typename FROM ca_objects_x_occurrences AS r ';
			$sql .= 'INNER JOIN ca_relationship_type_labels AS l ON l.type_id = r.type_id AND l.locale_id = 1 ';
			$sql .= 'WHERE r.occurrence_id = "'.$occurrence['id'].'" ';
			$sql .= 'LIMIT 1000;';

			$dbResult = $db->query($sql);
			
			while($dbResult->nextRow()) {
				$row = $dbResult->getRow();

				$objectId = $row['object_id'];
				
				$caObject = new ca_objects($objectId);
				if ($caObject->get('type_id') == 25) {
					$avCreationIds []= $objectId;
				} else if ($caObject->get('type_id') == 28) {
					$nonAvCreationIds []= $objectId;
				}
			}
			
			if (!empty($avCreationIds)) {
				$occurrence['avCreationIds'] = $avCreationIds;
			}
			
			if (!empty($nonAvCreationIds)) {
				$occurrence['nonAvCreationIds'] = $nonAvCreationIds;
			}
		}

		$response = [
			'events' => $caOccurrences,
			'placesByPlaceId' => $caPlacesByPlaceId
		];

		file_put_contents($file_path, json_encode($response));
		
		return ['response' => $response];
	}
}










