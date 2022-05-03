<?php
require_once(__CA_LIB_DIR__."/Service/BaseJSONService.php");

class ManifestService extends BaseJSONService {
	# -------------------------------------------------------
	public function __construct($po_request, $ps_table="") {
		parent::__construct($po_request, $ps_table);
	}
	# -------------------------------------------------------
	public function dispatch() {
		if ($this->getRequestMethod() == 'GET') {
			return $this->getManifest();
		} else {
			$this->addError(_t("Invalid HTTP request method"));
			return false;
		}
	}
	# -------------------------------------------------------
	protected function getManifest() {
		$vs_id = $this->opo_request->getParameter('id', pString);
		
		if(!($t_instance = $this->_getTableInstance($this->ops_table, $vs_id))) {
			return false;
		}

		if ($this->opo_request->getParameter("development", pString) == '1') {
			$vs_host = 'http://vhh-dev.test';	
		} else {
			$vs_host = $_SERVER['HTTP_ORIGIN'];
		}

		$va_return = array(
			'@context' => array(
				0 => 'http://iiif.io/api/presentation/2/context.json'
			),
			'@type' => 'sc:Manifest',
			'logo' => $vs_host.'/mmsi/images/vhh-logo.png',
			'service' => array()
		);
		
		$va_versions = ['original'];

		// VHH: Added $va_versions in order to get the URL of the original media file in this JSON view
		$o_service_config = Configuration::load(__CA_APP_DIR__."/conf/services.conf");
		$va_versions = $o_service_config->get('item_service_media_versions');
		if(!is_array($va_versions) || !sizeof($va_versions)) {
			$va_versions = ['preview170','original'];
		}

		$va_intrinsic_fields = $t_instance->getFieldsArray();
		
		// IDNO
		if (!empty($va_intrinsic_fields['idno'])) {
			$va_return['label'] = $t_instance->get('idno');
		}

		// Metadata - START
		$va_metadata = array();

		// preferred labels
		$va_labels = $t_instance->get($this->ops_table.".preferred_labels", array("returnWithStructure" => true, "returnAllLocales" => true, "assumeDisplayField" => false));
		$va_labels = end($va_labels);
		if(is_array($va_labels)) {
			foreach($va_labels as $vn_locale_id => $va_labels_by_locale) {
				foreach($va_labels_by_locale as $va_tmp) {
					foreach($t_instance->getLabelUIFields() as $vs_label_fld) {
						array_push($va_metadata, array('label' => 'Preferred Label', 'value' => $va_tmp[$vs_label_fld]));
					}
				}
			}
		}
		
		if (!empty($va_metadata)) {
			$va_return['metadata'] = $va_metadata;
		}
		// Metadata - END

		// Representation
		if($t_instance instanceof RepresentableBaseModel) {
			$va_reps = $t_instance->getRepresentations($va_versions);
			if(is_array($va_reps) && (sizeof($va_reps)>0)) {
				$va_allowed_reps = array();
				foreach($va_reps as $va_rep) {
					$pn_representation_id = $va_rep['representation_id'];
					$t_representation_instance = new ca_object_representations($pn_representation_id);
					
					$access_allowed = caCanRead(intval($this->opo_request->getUserID()), 'ca_object_representations', $va_rep['representation_id']);
					if($access_allowed) {
						array_push($va_allowed_reps, $va_rep);
					}
				}

				foreach($va_allowed_reps as $va_allowed_rep) {
					if ($va_allowed_rep['is_primary'] == '1'
						&& !empty($va_allowed_rep['urls'])
						&& !empty($va_allowed_rep['urls']['original'])) {
						$vs_original_url = $va_allowed_rep['urls']['original'];
						$vs_original_url = str_replace(array('http://ca/providence', 'https://ca/providence'), $vs_host.'/mmsi/api/ca', $vs_original_url);
						$va_return['@id'] = $vs_original_url;

						if (!empty($va_allowed_rep['urls']['preview170'])) {
							$vs_thumb_url = $va_allowed_rep['urls']['preview170'];
							$vs_thumb_url = str_replace(array('http://ca/providence', 'https://ca/providence'), $vs_host.'/mmsi/api/ca', $vs_thumb_url);
							$va_return['thumbnail'] = array(
								'@id' => $vs_thumb_url,
								'@type' => 'dctypes:Image'
							);
						}

						if ($va_allowed_rep['mimetype'] == 'application/pdf') {
							$va_return['mediaSequences'] = array(
								0 => array(
									'@id' => $vs_original_url,
									'@type' => 'ixif:MediaSequence',
									'label' => 'XSequence 0',
									'elements' => array(
										0 => array(
											'@id' => $vs_original_url,
											'@type' => 'foaf:Document',
											'format' => $va_allowed_rep['mimetype'],
											'label' => $va_return['label']
										)
									)
								)
							);
						} else if (!empty($va_allowed_rep['info']) and !empty($va_allowed_rep['info']['original'])) {
							$vs_iiif_url = $vs_host.'/mmsi/api/ca/service/IIIF/'.$va_allowed_rep['representation_id'];
							$vn_width = intval($va_allowed_rep['info']['original']['WIDTH']);
							$vn_height = intval($va_allowed_rep['info']['original']['HEIGHT']);
					
							$va_return['items'] = array(
								0 => array(
									'id' => $vs_iiif_url,
									'type' => 'Canvas',
						      'width' => $vn_width,
						      'height' => $vn_height,
									'items' => array(
										0 => array(
											'id' => $vs_iiif_url,
											'type' => 'AnnotationType',
											'items' => array(
												0 => array(
													'id' => $vs_iiif_url.'/full/max/0/default.jpg',
													'type' => 'Annotation',
													'target' => 'https://iiif.io/api/cookbook/recipe/0005-image-service/canvas/p1',
													'body' => array(
														'id' => $vs_iiif_url.'/full/max/0/default.jpg',
														'type' => 'Image',
														'format' => 'image/jpeg', //$va_allowed_rep['mimetype'],
														'width' => $vn_width,
								      			'height' => $vn_height,
														'service' => array(
															0 => array(
																'id' => $vs_iiif_url,
														    'profile' => 'level1',
														    'type' => 'ImageService3'
															)
														)
													)
												)
											)
										)
									)
								)
							);
						}
					}
				}
			}
		}

		return $va_return;
	}
}
