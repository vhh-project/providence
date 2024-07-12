<?php
/** ---------------------------------------------------------------------
 * app/lib/Service/ItemService.php
 * ----------------------------------------------------------------------
 * CollectiveAccess
 * Open-source collections management software
 * ----------------------------------------------------------------------
 *
 * Software by Whirl-i-Gig (http://www.whirl-i-gig.com)
 * Copyright 2012-2020 Whirl-i-Gig
 *
 * For more information visit http://www.CollectiveAccess.org
 *
 * This program is free software; you may redistribute it and/or modify it under
 * the terms of the provided license as published by Whirl-i-Gig
 *
 * CollectiveAccess is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTIES whatsoever, including any implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * This source code is free and modifiable under the terms of
 * GNU General Public License. (http://www.gnu.org/copyleft/gpl.html). See
 * the "license.txt" file for details, or visit the CollectiveAccess web site at
 * http://www.CollectiveAccess.org
 *
 * @package CollectiveAccess
 * @subpackage WebServices
 * @license http://www.gnu.org/copyleft/gpl.html GNU Public License version 3
 *
 * ----------------------------------------------------------------------
 */

 /**
  *
  */

require_once(__CA_LIB_DIR__."/Service/BaseJSONService.php");
require_once(__CA_MODELS_DIR__."/ca_lists.php");

class ItemService extends BaseJSONService {
  public $ENTITY_REPRESENTATION_CREATOR_TYPE_ID = "23";
  public $THUMBNAIL_TYPE_ID = "145";

	# -------------------------------------------------------
	public function __construct($po_request, $ps_table="") {
		parent::__construct($po_request, $ps_table);
	}
	# -------------------------------------------------------
	public function dispatch() {
		if ($this->getRequestMethod() == 'POST') {
			if ($this->opo_request->getParameter("uploadthumb", pString) == '1') {
				return $this->uploadThumb();
			} else {
				return $this->deleteThumb();
			}
		}

		switch($this->getRequestMethod()) {
			case "GET":
			case "POST":
				if ($this->opo_request->getParameter("lookup", pString) == '1') {
					return $this->getLookup();
				} else if(strlen($this->opn_id) > 0) {	// we allow that this might be a string here for idno-based fetching
					if(sizeof($this->getRequestBodyArray())==0) {
						// allow different format specifications
						if($vs_format = $this->opo_request->getParameter("format", pString)) {
							switch($vs_format) {
								// this one is tailored towards editing/adding the item
								// later, using the PUT variant of this service
								case 'edit':
									return $this->getItemInfoForEdit();
								case 'import':
									return $this->getItemInfoForImport();
								default:
									break;
							}
						}
						// fall back on default format
						return $this->getAllItemInfo();
					} else {
						return $this->getSpecificItemInfo();
					}
				} else {
					// do something here? (get all records!?)
					return array();
				}
				break;
			case "PUT":
				if(sizeof($this->getRequestBodyArray())==0) {
					$this->addError(_t("Missing request body for PUT"));
					return false;
				}
				if(strlen($this->opn_id) > 0) {   // we allow that this might be a string here for idno-based updating
					return $this->editItem();
				} else {
					return $this->addItem();
				}
				break;
			case "DELETE":
				if($this->opn_id>0) {
					return $this->deleteItem();
				} else {
					$this->addError(_t("No identifier specified"));
					return false;
				}
				break;
			default:
				$this->addError(_t("Invalid HTTP request method"));
				return false;
		}
	}
  # -------------------------------------------------------
	protected function getLookup() {
		$ps_query = $this->opo_request->getParameter('term', pString);
		$ps_bundle = $this->opo_request->getParameter('bundle', pString);

		$va_tmp = explode('.', $ps_bundle);
		
		if (!($t_table = Datamodel::getInstanceByTableName($va_tmp[0], true))) {
			// bad table name
			$this->addError(_t("Invalid table name"));
			return null;
		}

		$t_element = new ca_metadata_elements();
		if (!($t_element->load(array('element_code' => $va_tmp[1])))) {
			$this->addError(_t("Invalid element code"));
			return null;
		}
		
		if ((int)$t_element->getSetting('suggestExistingValues') !== 1) {
			$this->addError(_t("Value suggestion is not supported for this metadata element"));
			return null;
		}
		
		if ($this->opo_request->user->getBundleAccessLevel($va_tmp[0], $va_tmp[1]) == __CA_BUNDLE_ACCESS_NONE__) {
			$this->addError(_t("You do not have access to this bundle"));
			return null;
		}
		
		$va_type_restrictions = $t_element->getTypeRestrictions($t_table->tableNum());
		if (!$va_type_restrictions || !is_array($va_type_restrictions) || !sizeof($va_type_restrictions)) {
			$this->addError(_t("Element code is not bound to the specified table"));
			return null;
		}
		
		$o_db = new Db();
		
		switch($t_element->getSetting('suggestExistingValueSort')) {
			case 'recent':		// date/time entered
				$vs_sort_field = 'value_id DESC';
				$vn_max_returned_values = 10;
				break;
			default:				// alphabetically
				$vs_sort_field = 'value_longtext1 ASC';
				$vn_max_returned_values = 25;
				break;
		}
		
		$qr_res = $o_db->query("
			SELECT DISTINCT value_longtext1
			FROM ca_attribute_values
			WHERE
				element_id = ?
				AND
				(value_longtext1 LIKE ?)
			ORDER BY
				{$vs_sort_field}
			LIMIT {$vn_max_returned_values}
		", (int)$t_element->getPrimaryKey(), (string)$ps_query.'%');
		
		return array('response' => $qr_res->getAllFieldValues('value_longtext1'));
	}
	# -------------------------------------------------------
	protected function getSpecificItemInfo() {
		if(!($t_instance = $this->_getTableInstance($this->ops_table,$this->opn_id))) {	// note that $this->opn_id might be a string if we're fetching by idno; you can only use an idno for getting an item, not for editing or deleting
			return false;
		}

		$va_post = $this->getRequestBodyArray();

		$va_return = array();

		// allow user-defined template to be passed; allows flexible formatting of returned "display" value
		if (!($vs_template = $this->opo_request->getParameter('template', pString))) { $vs_template = ''; }
		if ($vs_template) {
			$va_return['display'] = caProcessTemplateForIDs($vs_template, $this->ops_table, array($this->opn_id));
		}

		if(!is_array($va_post["bundles"])) {
			return false;
		}
		foreach($va_post["bundles"] as $vs_bundle => $va_options) {
			if($this->_isBadBundle($vs_bundle)) {
				continue;
			}

			if(!is_array($va_options)) { $va_options = []; }

			// hack to add option to include comment count in search result
			if(trim($vs_bundle) == 'ca_item_comments.count') {
				$va_item[$vs_bundle] = (int) $t_instance->getNumComments(null);
				continue;
			}
			
			if(caGetOption('coordinates', $va_options, true)) { // Geocode attribute "coordinates" option returns an array which we want to serialize in the response, so we'll need to get it as a structured return value
				$va_options['returnWithStructure'] = true;
				unset($va_options['template']);	// can't use template with structured return value
			}

			$vm_return = $t_instance->get($vs_bundle,$va_options);
			
			if(caGetOption('returnWithStructure', $va_options, true)) {	// unroll structured response into flat list
				$vm_return = array_reduce($vm_return,
					function($c, $v) { 
						return array_merge($c, array_values($v));
					}, []
				);
			}

			// render 'empty' arrays as JSON objects, not as lists (which is the default behavior of json_encode)
			if(is_array($vm_return) && sizeof($vm_return)==0) {
				$va_return[$vs_bundle] = new stdClass;
			} else {
				$va_return[$vs_bundle] = $vm_return;
			}

		}

		return $va_return;
	}
	# -------------------------------------------------------
	/**
	 * Try to return a generic summary for the specified record
	 */
	protected function getAllItemInfo() {
		if(!($t_instance = $this->_getTableInstance($this->ops_table,$this->opn_id))) {	// note that $this->opn_id might be a string if we're fetching by idno; you can only use an idno for getting an item, not for editing or deleting
			return false;
		}
		
		$o_service_config = Configuration::load(__CA_APP_DIR__."/conf/services.conf");
		$va_versions = $o_service_config->get('item_service_media_versions');
		if(!is_array($va_versions) || !sizeof($va_versions)) { $va_versions = ['preview170','large','original']; }

		$t_list = new ca_lists();
		$t_locales = new ca_locales();
		$va_locales = $t_locales->getLocaleList(array("available_for_cataloguing_only" => true));

		$va_return = array();

		// get options
		$va_get_options = $this->opo_request->getParameter('getOptions', pArray);
		if(!is_array($va_get_options)) { $va_get_options = array(); }
		$va_get_options = array_merge(array("returnWithStructure" => true, "returnAllLocales" => true), $va_get_options);

		// allow user-defined template to be passed; allows flexible formatting of returned "display" value
		if (!($vs_template = $this->opo_request->getParameter('template', pString))) { $vs_template = ''; }
		if ($vs_template) {
			$va_return['display'] = caProcessTemplateForIDs($vs_template, $this->ops_table, array($this->opn_id), $va_get_options);
		}

		// labels
		$va_labels = $t_instance->get($this->ops_table.".preferred_labels", $va_get_options);
		$va_labels = end($va_labels);
		if(is_array($va_labels)) {
			foreach ($va_labels as $vn_locale_id => $va_labels_by_locale) {
				foreach ($va_labels_by_locale as $vs_tmp) {
					$va_return["preferred_labels"][$va_locales[$vn_locale_id]["code"]][] = $vs_tmp;
				}
			}
		}

		$va_labels = $t_instance->get($this->ops_table.".nonpreferred_labels", $va_get_options);
		$va_labels = end($va_labels);
		if(is_array($va_labels)) {
			foreach($va_labels as $vn_locale_id => $va_labels_by_locale) {
				foreach($va_labels_by_locale as $vs_tmp) {
					$va_return["nonpreferred_labels"][$va_locales[$vn_locale_id]["code"]][] = $vs_tmp;
				}
			}
		}

		// "intrinsic" fields
		foreach($t_instance->getFieldsArray() as $vs_field_name => $va_field_info) {
			if (($this->ops_table == 'ca_object_representations') && ($vs_field_name == 'media_metadata')) { continue; }
			$vs_list = null;

			// this is way to complicated. @todo: get() handles all of that now I think
			if(!is_null($vs_val = $t_instance->get($vs_field_name))) {
				$va_return[$vs_field_name] = array(
					"value" => $vs_val,
				);
				if(isset($va_field_info["LIST"])) { // fields like "access" and "status"
					$va_tmp = end($t_list->getItemFromListByItemValue($va_field_info["LIST"],$vs_val));
					foreach($va_locales as $vn_locale_id => $va_locale) {
						$va_return[$vs_field_name]["display_text"][$va_locale["code"]] =
							$va_tmp[$vn_locale_id]["name_singular"];
					}
				}
				if(isset($va_field_info["LIST_CODE"])) { // typical example: type_id
					$va_item = $t_list->getItemFromListByItemID($va_field_info["LIST_CODE"],$vs_val);
					$t_item = new ca_list_items($va_item["item_id"]);
					$va_labels = $t_item->getLabels(null,__CA_LABEL_TYPE_PREFERRED__);
					foreach($va_locales as $vn_locale_id => $va_locale) {
						if($vs_label = $va_labels[$va_item["item_id"]][$vn_locale_id][0]["name_singular"]) {
							$va_return[$vs_field_name]["display_text"][$va_locale["code"]] = $vs_label;
						}
					}
				}
				switch($vs_field_name) {
					case 'parent_id':
						if($t_parent = $this->_getTableInstance($this->ops_table, $vs_val)) {
							$va_return['intrinsic'][$vs_field_name] = $t_parent->get('idno');
						}
						break;
					default:
						$va_return['intrinsic'][$vs_field_name] = $vs_val;
						break;
				}
			}
		}

		// comment count
		$va_return['num_comments'] = $t_instance->getNumComments(null);

		// representations for representable stuff
		if($t_instance instanceof RepresentableBaseModel) {
			$va_reps = $t_instance->getRepresentations($va_versions);
			if(is_array($va_reps) && (sizeof($va_reps)>0)) {
				$va_return['representations'] = caSanitizeArray($va_reps, ['removeNonCharacterData' => true]);
			}
		}

		// captions for representations
		if($t_instance instanceof ca_object_representations) {
			$va_captions = $t_instance->getCaptionFileList();
			if(is_array($va_captions) && (sizeof($va_captions)>0)) {
				$va_return['captions'] = $va_captions;
			}
		}

		// attributes
		$va_codes = $t_instance->getApplicableElementCodes();
		foreach($va_codes as $vs_code) {
			if($va_vals = $t_instance->get(
				$this->ops_table.".".$vs_code,
				array_merge(array("convertCodesToDisplayText" => true), $va_get_options))
			) {
				$va_vals_by_locale = end($va_vals);
				$va_attribute_values = array();
				foreach($va_vals_by_locale as $vn_locale_id => $va_locale_vals) {
					if(!is_array($va_locale_vals)) { continue; }
					foreach($va_locale_vals as $vs_val_id => $va_actual_data) {
						$vs_locale_code = isset($va_locales[$vn_locale_id]["code"]) ? $va_locales[$vn_locale_id]["code"] : "none";
						$va_attribute_values[$vs_val_id][$vs_locale_code] = $va_actual_data;
					}

					$va_return[$this->ops_table.".".$vs_code] = $va_attribute_values;
				}
			}
		}

		// relationships
		// yes, not all combinations between these tables have
		// relationships but it also doesn't hurt to query
		foreach($this->opa_valid_tables as $vs_rel_table) {
			$vs_get_spec = $vs_rel_table;
			if($vs_rel_table == $this->ops_table) {
				$vs_get_spec = $vs_rel_table . '.related';
			}

			//
			// set-related hacks
			if($this->ops_table == "ca_sets" && $vs_rel_table=="ca_tours") { // throws SQL error in getRelatedItems
				continue;
			}
			// you'd expect the set items to be included for sets but
			// we don't wan't to list set items as allowed related table
			// which is why we add them by hand here
			if($this->ops_table == "ca_sets") {
				$va_tmp = $t_instance->getItems();
				$va_set_items = array();
				foreach($va_tmp as $va_loc) {
					foreach($va_loc as $va_item) {
						$va_set_items[] = $va_item;
					}
				}
				$va_return["related"]["ca_set_items"] = $va_set_items;
			}
			// end set-related hacks
			//

			$va_related_items = $t_instance->get($vs_get_spec, $va_get_options);
			$t_rel_instance = Datamodel::getInstance($vs_rel_table);

			if(is_array($va_related_items) && sizeof($va_related_items)>0) {
				if($t_rel_instance instanceof RepresentableBaseModel) {
					foreach($va_related_items as &$va_rel_item) {
						if($t_rel_instance->load($va_rel_item[$t_rel_instance->primaryKey()])) {
							$va_rel_item['representations'] = caSanitizeArray($t_rel_instance->getRepresentations($va_versions), ['removeNonCharacterData' => true]);
						}
					}
				}
				$va_return["related"][$vs_rel_table] = array_values($va_related_items);
			}
		}

		return $va_return;
	}
	# -------------------------------------------------------
	/**
	 * Get a record summary that looks reasonably close to what we expect to be passed to the
	 * PUT portion of this very service. With this hack editing operations should be easier to handle.
	 */
	private function getItemInfoForEdit() {
		if(!($t_instance = $this->_getTableInstance($this->ops_table,$this->opn_id))) {
			return false;
		}
		$t_locales = new ca_locales();

		$va_locales = $t_locales->getLocaleList(array("available_for_cataloguing_only" => true));

		$va_return = array();

		// VHH: Added $va_versions in order to get the URL of the original media file in this JSON view
		$o_service_config = Configuration::load(__CA_APP_DIR__."/conf/services.conf");
		$va_versions = $o_service_config->get('item_service_media_versions');
		if(!is_array($va_versions) || !sizeof($va_versions)) {
			$va_versions = ['preview170','large','original'];
		}

		// allow user-defined template to be passed; allows flexible formatting of returned "display" value
		if (!($vs_template = $this->opo_request->getParameter('template', pString))) { $vs_template = ''; }
		if ($vs_template) {
			$va_return['display'] = caProcessTemplateForIDs($vs_template, $this->ops_table, array($this->opn_id));
		}

		// "intrinsic" fields
		foreach($t_instance->getFieldsArray() as $vs_field_name => $va_field_info) {
			$vs_list = null;
			if(!is_null($vs_val = $t_instance->get($vs_field_name))) {
				if(preg_match("/^hier\_/",$vs_field_name)) { continue; }
				if(preg_match("/\_sort$/",$vs_field_name)) { continue; }
				//if($vs_field_name == $t_instance->primaryKey()) { continue; }
				$va_return['intrinsic_fields'][$vs_field_name] = $vs_val;
			}
		}

		// preferred labels
		$va_labels = $t_instance->get($this->ops_table.".preferred_labels", array("returnWithStructure" => true, "returnAllLocales" => true, "assumeDisplayField" => false));
		$va_labels = end($va_labels);
		if(is_array($va_labels)) {
			foreach($va_labels as $vn_locale_id => $va_labels_by_locale) {
				foreach($va_labels_by_locale as $va_tmp) {
					$va_label = array();
					$va_label['locale'] = $va_locales[$vn_locale_id]["code"];

					// add only UI fields to return
					foreach($t_instance->getLabelUIFields() as $vs_label_fld) {
						$va_label[$vs_label_fld] = $va_tmp[$vs_label_fld];
					}

					$va_return["preferred_labels"][] = $va_label;
				}
			}
		}

		// nonpreferred labels
		$va_labels = $t_instance->get($this->ops_table.".nonpreferred_labels", array("returnWithStructure" => true, "returnAllLocales" => true, "assumeDisplayField" => false));
		$va_labels = end($va_labels);
		if(is_array($va_labels)) {
			foreach($va_labels as $vn_locale_id => $va_labels_by_locale) {
				foreach($va_labels_by_locale as $va_tmp) {
					$va_label = array();
					$va_label['locale'] = $va_locales[$vn_locale_id]["code"];

					// add only UI fields to return
					foreach($t_instance->getLabelUIFields() as $vs_label_fld) {
						$va_label[$vs_label_fld] = $va_tmp[$vs_label_fld];
					}

					$va_return["nonpreferred_labels"][] = $va_label;
				}
			}
		}

		// representations for representable stuff
		// VHH: Added $va_versions in order to show the path of the  original media
		// VHH: Added ACL restrictions
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
				$va_return['representations'] = caSanitizeArray($va_allowed_reps, ['removeNonCharacterData' => true]);
			}
		}

		// captions for representations
		if($this->ops_table == "ca_object_representations") {
			$va_captions = $t_instance->getCaptionFileList();
			if(is_array($va_captions) && (sizeof($va_captions)>0)) {
				$va_return['captions'] = $va_captions;
			}
		}

		// attributes
		// VHH CHANGES - added an option in order to get both - ids and labels for entities, lists, etc.
		$va_codes = $t_instance->getApplicableElementCodes();
		foreach($va_codes as $vs_code) {
			if($va_vals = $t_instance->get($this->ops_table.".".$vs_code,
				array("returnWithStructure" => true, "returnAllLocales" => true, "convertCodesToDisplayText" => false))
			 ){
				if (!empty($_GET["showdisplaytext"]) && $_GET["showdisplaytext"] === '1') {
					$va_display_vals_original = $t_instance->get($this->ops_table.".".$vs_code,
						array("returnWithStructure" => true, "returnAllLocales" => true, "convertCodesToDisplayText" => true));
					$va_display_vals_ordered = array();
					$va_display_vals_original_locale = end($va_display_vals_original);
					foreach($va_display_vals_original_locale as $vn_locale_id => $va_locale_vals) {
						foreach($va_locale_vals as $vs_val_id => $va_actual_data) {
							if(!is_array($va_actual_data)) {
								continue;
							}
							$vs_locale_code = isset($va_locales[$vn_locale_id]["code"]) ? $va_locales[$vn_locale_id]["code"] : "none";
							if (!isset($va_display_vals_ordered[$vs_val_id])) {
								$va_display_vals_ordered[$vs_val_id] = array();
							}
							$va_display_vals_ordered[$vs_val_id][$vs_locale_code] = $va_actual_data;
						}
					}
				}
				$va_vals_by_locale = end($va_vals); // I seriously have no idea what that additional level of nesting in the return format is for
				foreach($va_vals_by_locale as $vn_locale_id => $va_locale_vals) {
					foreach($va_locale_vals as $vs_val_id => $va_actual_data) {
						if(!is_array($va_actual_data)) {
							continue;
						}
						$vs_locale_code = isset($va_locales[$vn_locale_id]["code"]) ? $va_locales[$vn_locale_id]["code"] : "none";

						// VHH CHANGES
						// Adding the actual id of the attribute
						// Adding the value_source field for the attribute
						$t_attr = new ca_attributes($vs_val_id);
						$valueSource = (!empty($t_attr->_FIELD_VALUES) && !empty($t_attr->_FIELD_VALUES['value_source'])) ? $t_attr->_FIELD_VALUES['value_source'] : '';

						if (isset($va_display_vals_ordered)) {
							if (isset($va_display_vals_ordered[$vs_val_id]) && isset($va_display_vals_ordered[$vs_val_id][$vs_locale_code])) {
								$va_display_data = $va_display_vals_ordered[$vs_val_id][$vs_locale_code];
							} else {
								$va_display_data = array();
							}

							$va_final_data = array();

							foreach ($va_actual_data as $attr_key => $attr_value) {
								$va_display_value = (isset($va_display_data[$attr_key])) ? $va_display_data[$attr_key] : null;
								$va_final_data[$attr_key] = array('data' => $attr_value, 'label' => $va_display_value);
							}
							$va_return['attributes'][$vs_code][] = array_merge(array('locale' => $vs_locale_code, '_id' => $vs_val_id, '_value_source' => $valueSource), $va_final_data);
						} else {
							$va_return['attributes'][$vs_code][] = array_merge(array('locale' => $vs_locale_code, '_id' => $vs_val_id, '_value_source' => $valueSource), $va_actual_data);
						}
						// VHH CHANGES -- END
					}

				}
			}
		}

		// relationships
		// yes, not all combinations between these tables have
		// relationships but it also doesn't hurt to query

		// VHH - START
		// Possibility to define attributes to be added to a relationship
		$va_add_rel_info = [];
		if (!empty($_GET["add_relation_info"])) {
			foreach(explode(';', $_GET["add_relation_info"]) as $vs_rel_info) {
				$va_rel_info = explode('.', $vs_rel_info);
				if (count($va_rel_info) == 2) {
					$vs_rel_type = $va_rel_info[0];
					$vs_rel_attr = $va_rel_info[1];
					if (empty($va_add_rel_info[$vs_rel_type])) {
						$va_add_rel_info[$vs_rel_type] = [];
					}

					$va_add_rel_info[$vs_rel_type][] = $vs_rel_attr;
				}
			}
		}
		// VHH - END

		foreach($this->opa_valid_tables as $vs_rel_table) {
			$vs_get_spec = $vs_rel_table;
			if($vs_rel_table == $this->ops_table) {
				$vs_get_spec = $vs_rel_table . '.related';
			}

			//
			// set-related hacks
			if($this->ops_table == "ca_sets" && $vs_rel_table=="ca_tours") { // throw SQL error in getRelatedItems
				continue;
			}
			$va_related_items = $t_instance->get($vs_get_spec, array("returnWithStructure" => true, 'limit' => 1000));
			if(is_array($va_related_items) && sizeof($va_related_items)>0) {
				// most of the fields are usually empty because they are not supported on UI level
				foreach($va_related_items as $va_rel_item) {
					$va_item_add = array();
					foreach($va_rel_item as $vs_fld => $vs_val) {
						if((!is_array($vs_val)) && strlen(trim($vs_val))>0) {
							// rewrite and ignore certain field names
							switch($vs_fld) {
								case 'relationship_type_id':
									$va_item_add['type_id'] = $vs_val;
									break;
								default:
									$va_item_add[$vs_fld] = $vs_val;
									break;
							}
						}
					}

					// VHH - START
					// Add item type name (e.g. 'AV Manifestation')
					if (!empty($va_item_add['item_type_id'])) {
						$va_item_add['item_type_name'] = $t_instance->getTypeName($va_item_add['item_type_id']);
					}
					// VHH - END

					// VHH - START
					// Possibility to define attributes to be added to a relationship
					if (!empty($va_add_rel_info[$vs_rel_table])) {
						$va_item_add['related_attributes'] = [];
						$vs_id_string = null;

						switch($vs_rel_table) {
							case 'ca_objects':
								$vs_id_string = 'object_id';
								break;
							case 'ca_entities':
								$vs_id_string = 'entity_id';
								break;
							case 'ca_occurrences':
								$vs_id_string = 'occurrence_id';
								break;
							case 'ca_places':
								$vs_id_string = 'place_id';
								break;
							case 'ca_collections':
								$vs_id_string = 'collection_id';
								break;
						}

						if (!empty($vs_id_string)) {
							if (($t_rel_subject = Datamodel::getInstanceByTableName($vs_rel_table))) {
								$t_rel_subject->load($va_rel_item[$vs_id_string]);
								$t_rel_subject->reloadLabelDefinitions();

								foreach($va_add_rel_info[$vs_rel_table] as $vs_rel_attr_code) {
									if ($va_vals = $t_rel_subject->get($vs_rel_table.".".$vs_rel_attr_code,
											array("returnWithStructure" => true, "returnAllLocales" => true, "convertCodesToDisplayText" => false))
										 )
									{
										$va_item_add['related_attributes'][$vs_rel_attr_code] = $va_vals;
										if (!empty($va_vals)) {
											$attrList = array();
											$va_vals = end($va_vals);
											$va_vals = end($va_vals);
											foreach($va_vals as $attrId => $attrs) {
												$t_attr = new ca_attributes($attrId);
												$attrs['_id'] = $attrId;
												$attrList[] = $attrs;
											}

											$va_item_add['related_attributes'][$vs_rel_attr_code] = $attrList;
										}
									}
								}
							}
						}
					}

					// VHH: Add interstitial attributes to relations

					if (!empty($va_item_add['relation_id'])) {
						$vs_rel_table_name = null;

						if ($this->ops_table == 'ca_objects') {
							if ($vs_rel_table == 'ca_objects') {
								$vs_rel_table_name = 'ca_objects_x_objects';
							}
							else if ($vs_rel_table == 'ca_entities') {
								$vs_rel_table_name = 'ca_objects_x_entities';
							}
							else if ($vs_rel_table == 'ca_occurrences') {
								$vs_rel_table_name = 'ca_objects_x_occurrences';
							}
							else if ($vs_rel_table == 'ca_places') {
								$vs_rel_table_name = 'ca_objects_x_places';
							}
							else if ($vs_rel_table == 'ca_collections') {
								$vs_rel_table_name = 'ca_objects_x_collections';
							}
						} else if ($this->ops_table == 'ca_entities') {
							if ($vs_rel_table == 'ca_objects') {
								$vs_rel_table_name = 'ca_objects_x_entities';
							}
							else if ($vs_rel_table == 'ca_entities') {
								$vs_rel_table_name = 'ca_entities_x_entities';
							}
							else if ($vs_rel_table == 'ca_occurrences') {
								$vs_rel_table_name = 'ca_entities_x_occurrences';
							}
							else if ($vs_rel_table == 'ca_places') {
								$vs_rel_table_name = 'ca_entities_x_places';
							}
							else if ($vs_rel_table == 'ca_collections') {
								$vs_rel_table_name = 'ca_entities_x_collections';
							}
						} else if ($this->ops_table == 'ca_occurrences') {
							if ($vs_rel_table == 'ca_objects') {
								$vs_rel_table_name = 'ca_objects_x_occurrences';
							}
							else if ($vs_rel_table == 'ca_entities') {
								$vs_rel_table_name = 'ca_entities_x_occurrences';
							}
							else if ($vs_rel_table == 'ca_occurrences') {
								$vs_rel_table_name = 'ca_occurrences_x_occurrences';
							}
							else if ($vs_rel_table == 'ca_places') {
								$vs_rel_table_name = 'ca_places_x_occurrences';
							}
							else if ($vs_rel_table == 'ca_collections') {
								$vs_rel_table_name = 'ca_occurrences_x_collections';
							}
						} else if ($this->ops_table == 'ca_places') {
							if ($vs_rel_table == 'ca_objects') {
								$vs_rel_table_name = 'ca_objects_x_places';
							}
							else if ($vs_rel_table == 'ca_entities') {
								$vs_rel_table_name = 'ca_entities_x_places';
							}
							else if ($vs_rel_table == 'ca_occurrences') {
								$vs_rel_table_name = 'ca_places_x_occurrences';
							}
							else if ($vs_rel_table == 'ca_places') {
								$vs_rel_table_name = 'ca_places_x_places';
							}
							else if ($vs_rel_table == 'ca_collections') {
								$vs_rel_table_name = 'ca_places_x_collections';
							}
						} else if ($this->ops_table == 'ca_collections') {
							if ($vs_rel_table == 'ca_objects') {
								$vs_rel_table_name = 'ca_objects_x_collections';
							}
							else if ($vs_rel_table == 'ca_entities') {
								$vs_rel_table_name = 'ca_entities_x_collections';
							}
							else if ($vs_rel_table == 'ca_occurrences') {
								$vs_rel_table_name = 'ca_occurrences_x_collections';
							}
							else if ($vs_rel_table == 'ca_places') {
								$vs_rel_table_name = 'ca_places_x_collections';
							}
							else if ($vs_rel_table == 'ca_collections') {
								$vs_rel_table_name = 'ca_collections_x_collections';
							}
						}

						if (isset($vs_rel_table_name)) {
							if (($t_subject = Datamodel::getInstanceByTableName($vs_rel_table_name))) {
								$t_subject->load($va_item_add['relation_id']);
								$t_subject->reloadLabelDefinitions();
								$va_codes = $t_subject->getApplicableElementCodes();
									foreach($va_codes as $vs_code) {
										if($va_vals = $t_subject->get($vs_rel_table_name.".".$vs_code,
											array("returnWithStructure" => true, "returnAllLocales" => true, "convertCodesToDisplayText" => false))
										 ) {
											if (!empty($va_vals)) {
												$attrList = array();
												$va_vals = end($va_vals);
												$va_vals = end($va_vals);
												foreach($va_vals as $attrId => $attrs) {
													$t_attr = new ca_attributes($attrId);
													$valueSource = (!empty($t_attr->_FIELD_VALUES) && !empty($t_attr->_FIELD_VALUES['value_source'])) ? $t_attr->_FIELD_VALUES['value_source'] : '';
													$attrs['_id'] = $attrId;
													$attrs['_value_source'] = $valueSource;
													$attrList[] = $attrs;
												}

												if (!isset($va_item_add['attributes'])) {
													$va_item_add['attributes'] = array();
												}

												$va_item_add['attributes'][$vs_code] = $attrList;
											}
										}
									}
								}
							}
					}

					$va_return["related"][$vs_rel_table][] = $va_item_add;
				}
			}
		}

		return $va_return;
	}
	# -------------------------------------------------------
	/**
	 * Get a record summary that is easier to parse when importing to another system
	 */
	private function getItemInfoForImport() {
		if(!($t_instance = $this->_getTableInstance($this->ops_table,$this->opn_id))) {
			return false;
		}

		$t_list = new ca_lists();
		$t_locales = new ca_locales();

		//
		// Options
		//
		if (!($vs_delimiter = $this->opo_request->getParameter('delimiter', pString))) { $vs_delimiter = "; "; }
		if (!($vs_flatten = $this->opo_request->getParameter('flatten', pString))) { $vs_flatten = null; }
		$va_flatten = array_flip(preg_split("![ ]*[;]+[ ]*!", $vs_flatten));

		$va_locales = $t_locales->getLocaleList(array("available_for_cataloguing_only" => true));

		$va_return = array();

		// allow user-defined template to be passed; allows flexible formatting of returned "display" value
		if (!($vs_template = $this->opo_request->getParameter('template', pString))) { $vs_template = ''; }
		if ($vs_template) {
			$va_return['display'] = caProcessTemplateForIDs($vs_template, $this->ops_table, array($this->opn_id));
		}

		// "intrinsic" fields
		
		$type_id_fld_name = $t_instance->getTypeFieldName();
		foreach($t_instance->getFieldsArray() as $vs_field_name => $va_field_info) {
			$vs_list = null;
			if(!is_null($vs_val = $t_instance->get($vs_field_name))) {
				if(preg_match("/^hier\_/",$vs_field_name)) { continue; }
				if(preg_match("/\_sort$/",$vs_field_name)) { continue; }
				//if($vs_field_name == $t_instance->primaryKey()) { continue; }

				if(isset($va_field_info["LIST_CODE"])) { // typical example: type_id
					$va_item = $t_list->getItemFromListByItemID($va_field_info["LIST_CODE"],$vs_val);
					if ($t_item = new ca_list_items($va_item["item_id"])) {
						$vs_val = $t_item->get('idno');
					}
				} elseif($vs_field_name == $type_id_fld_name) {
				    $vs_val = $t_instance->getTypeCodeForID($va_item["item_id"]);
				}
				switch($vs_field_name) {
					case 'parent_id':
						if($t_parent = $this->_getTableInstance($this->ops_table, $vs_val)) {
							$va_return['intrinsic'][$vs_field_name] = $t_parent->get('idno');
						}
						break;
					default:
						$va_return['intrinsic'][$vs_field_name] = $vs_val;
						break;
				}
			}
		}
		
		// tags
		if(is_array($tags = $t_instance->getTags()) && sizeof($tags)) {
		    $va_return['tags'] = $tags;
        } else {
            $va_return['tags'] = [];
        }
        
		// preferred labels
		$va_labels = $t_instance->get($this->ops_table.".preferred_labels",array("returnWithStructure" => true, "assumeDisplayField" => false, "returnAllLocales" => true));
		$va_labels = end($va_labels);

		$vs_display_field_name = $t_instance->getLabelDisplayField();

		if(is_array($va_labels)) {
			foreach($va_labels as $vn_locale_id => $va_labels_by_locale) {
				foreach($va_labels_by_locale as $va_tmp) {
					$va_label = array();
					$va_label['locale'] = $va_locales[$vn_locale_id]["code"];

					// add only UI fields to return
					foreach(array_merge($t_instance->getLabelUIFields(), array('type_id')) as $vs_label_fld) {
						$va_label[$vs_label_fld] = $va_tmp[$vs_label_fld];
					}
					$va_label[$vs_label_fld] = $va_tmp[$vs_label_fld];
					$va_label['label'] = $va_tmp[$vs_display_field_name];

					$va_return["preferred_labels"][$va_label['locale']] = $va_label;
				}
			}

			if (isset($va_flatten['locales'])) {
				$va_return["preferred_labels"] = array_pop(caExtractValuesByUserLocale(array($va_return["preferred_labels"])));
			}
		} else {
		    $va_return["preferred_labels"] = [];
		}
		
		// preferred labels hierarchy
		$va_labels = $t_instance->get($this->ops_table.".hierarchy.preferred_labels",array("returnWithStructure" => true, "assumeDisplayField" => true, "returnAllLocales" => true));
		$va_labels = end($va_labels);

		$vs_display_field_name = $t_instance->getLabelDisplayField();

		if(is_array($va_labels)) {
			foreach($va_labels as $vn_locale_id => $va_labels_by_locale) {
				foreach($va_labels_by_locale as $va_tmp) {
					$va_label = array();
					$va_label['locale'] = $va_locales[$vn_locale_id]["code"];

					// add only UI fields to return
					foreach(array_merge($t_instance->getLabelUIFields(), array('type_id')) as $vs_label_fld) {
						$va_label[$vs_label_fld] = $va_tmp[$vs_label_fld];
					}
					$va_label[$vs_label_fld] = $va_tmp[$vs_label_fld];
					$va_label['label'] = $va_tmp[$vs_display_field_name];

					$va_return["preferred_labels_hierarchy"][$va_label['locale']] = $va_label;
				}
			}

			if (isset($va_flatten['locales'])) {
				$va_return["preferred_labels_hierarchy"] = array_pop(caExtractValuesByUserLocale(array($va_return["preferred_labels_hierarchy"])));
			}
		} else {
		    $va_return["preferred_labels_hierarchy"] = [];
		}

		// nonpreferred labels
		$va_labels = $t_instance->get($this->ops_table.".nonpreferred_labels",array("returnWithStructure" => true, "returnAllLocales" => true));
		$va_labels = end($va_labels);
		if(is_array($va_labels)) {
			foreach($va_labels as $vn_locale_id => $va_labels_by_locale) {
				foreach($va_labels_by_locale as $va_tmp) {
					$va_label = array('locale' => $va_locales[$vn_locale_id]["code"]);

					// add only UI fields to return
					foreach(array_merge($t_instance->getLabelUIFields(), array('type_id')) as $vs_label_fld) {
						$va_label[$vs_label_fld] = $va_tmp[$vs_label_fld];
					}

					$va_return["nonpreferred_labels"][] = $va_label;
				}
			}

			if (isset($va_flatten['locales'])) {
				$va_return["nonpreferred_labels"] = array_pop(caExtractValuesByUserLocale(array($va_return["nonpreferred_labels"])));
			}
		} else {
		    $va_return["nonpreferred_labels"] = [];
		}

		// attributes
		$va_codes = $t_instance->getApplicableElementCodes();
		foreach($va_codes as $vs_code) {

			if($va_vals = $t_instance->get($this->ops_table.".".$vs_code,
				array("convertCodesToDisplayText" => false, "returnWithStructure" => true, "returnAllLocales" => true))
			 ){
				$va_vals_as_text = end($t_instance->get($this->ops_table.".".$vs_code,
					array("convertCodesToDisplayText" => true, "returnWithStructure" => true, "returnAllLocales" => true)));
				$va_vals_by_locale = end($va_vals);
				foreach($va_vals_by_locale as $vn_locale_id => $va_locale_vals) {
					foreach($va_locale_vals as $vs_val_id => $va_actual_data) {
						if(!is_array($va_actual_data)) {
							continue;
						}

						$vs_locale_code = isset($va_locales[$vn_locale_id]["code"]) ? $va_locales[$vn_locale_id]["code"] : "none";

						foreach($va_actual_data as $vs_f => $vs_v) {
							if (isset($va_vals_as_text[$vn_locale_id][$vs_val_id][$vs_f]) && ($vs_v != $va_vals_as_text[$vn_locale_id][$vs_val_id][$vs_f])) {
								$va_actual_data[$vs_f.'_display'] = $va_vals_as_text[$vn_locale_id][$vs_val_id][$vs_f];

								if ($vs_item_idno = caGetListItemIdno($va_actual_data[$vs_f])) {
									$va_actual_data[$vs_f] = $vs_item_idno;
								}
							}
						}


						$va_return['attributes'][$vs_code][$vs_locale_code][] = array_merge(array('locale' => $vs_locale_code),$va_actual_data);
					}
				}
			}
		}
		if (isset($va_flatten['locales'])) {
			$va_return['attributes'] = caExtractValuesByUserLocale($va_return['attributes']);
		}

		// relationships
		// yes, not all combinations between these tables have
		// relationships but it also doesn't hurt to query
		foreach($this->opa_valid_tables as $vs_rel_table) {
		    if (!$t_rel = Datamodel::getInstance($vs_rel_table, true)) { continue; }
		    $type_id_fld_name = $t_instance->getTypeFieldName();
			$vs_get_spec = $vs_rel_table;
			if($vs_rel_table == $this->ops_table) {
				$vs_get_spec = $vs_rel_table . '.related';
			}

			//
			// set-related hacks
			if(($this->ops_table == "ca_sets") && ($vs_rel_table=="ca_tours")) { // throw SQL error in getRelatedItems
				continue;
			}

			$va_related_items = $t_instance->getRelatedItems($vs_rel_table,array('returnWithStructure' => true, 'returnAsArray' => true, 'useLocaleCodes' => true, 'groupFields' => true, 'limit' => 100));
           
			if(($this->ops_table == "ca_objects") && ($vs_rel_table=="ca_object_representations")) {
				$va_versions = $t_instance->getMediaVersions('media');

				if (isset($va_flatten['all'])) {
					$va_reps = $t_instance->getRepresentations(array('original'));
					$va_urls = array();
					foreach($va_reps as $vn_i => $va_rep) {
						$va_urls[] = $va_rep['urls']['original'];
					}
					$va_return['representations'] = join($vs_delimiter, $va_urls);
				} else {
					$va_return['representations'] = caSanitizeArray($t_instance->getRepresentations(['original']), ['removeNonCharacterData' => true]);
				}

				if(is_array($va_return['representations'])) {
					foreach($va_return['representations'] as $vn_i => $va_rep) {
						unset($va_return['representations'][$vn_i]['media']);
						unset($va_return['representations'][$vn_i]['media_metadata']);
					}
				}
			}

			if(is_array($va_related_items) && sizeof($va_related_items)>0) {
				foreach($va_related_items as $va_rel_item) {
					$va_item_add = array();
					foreach($va_rel_item as $vs_fld => $vs_val) {
						if((!is_array($vs_val)) && strlen(trim($vs_val))>0) {
							// rewrite and ignore certain field names
							switch($vs_fld) {
								case 'item_type_id':
									$va_item_add[$vs_fld] = $vs_val;
									$va_item_add['type_id'] = $vs_val;
									$va_item_add['type_id_code'] = $t_rel->getTypeCodeForID($vs_val);
									break;
								case 'item_source_id':
									$va_item_add[$vs_fld] = $vs_val;
									$va_item_add['source_id'] = $vs_val;
									break;
								default:
									$va_item_add[$vs_fld] = $vs_val;
									break;
							}
						} else {
							if (in_array($vs_fld, array('preferred_labels', 'intrinsic'))) {
								$va_item_add[$vs_fld] = $vs_val;
							}
						}
						
						if ($vs_fld == 'preferred_labels') {
						    $q = caMakeSearchResult($vs_rel_table, [$va_rel_item[Datamodel::primaryKey($vs_rel_table)]]);
						    if ($q->nextHit()) {
						        $va_item_add['preferred_labels_hierarchy'] = $q->get("{$vs_rel_table}.hierarchy.preferred_labels", ['returnAsArray' => true]);
						    }
						    
						}
					}
					if ($vs_rel_table=="ca_object_representations") {
						$t_rep = new ca_object_representations($va_rel_item['representation_id']);
						$va_item_add['media'] = $t_rep->getMediaUrl('media', 'original');
					}
					$va_return["related"][$vs_rel_table][] = $va_item_add;
				}
			}
		}

		return $va_return;
	}
	# -------------------------------------------------------
	/**
	 * Add item as specified in request body array. Can also be used to
	 * add item directly. If both parameters are set, the request data
	 * is ignored.
	 * @param null|string $ps_table optional table name. if not set, table name is taken from request
	 * @param null|array $pa_data optional array with item data. if not set, data is taken from request body
	 * @return array|bool
	 */
	public function addItem($ps_table=null, $pa_data=null) {
		if(!$ps_table) { $ps_table = $this->ops_table; }
		if(!($t_instance = $this->_getTableInstance($ps_table))) {
			return false;
		}

		$t_locales = new ca_locales();
		if(!$pa_data || !is_array($pa_data)) { $pa_data = $this->getRequestBodyArray(); }

		// intrinsic fields
		if(is_array($pa_data["intrinsic_fields"]) && sizeof($pa_data["intrinsic_fields"])) {
			foreach($pa_data["intrinsic_fields"] as $vs_field_name => $vs_value) {
				$t_instance->set($vs_field_name,$vs_value);
			}
		} else {
			$this->addError(_t("No intrinsic fields specified"));
			return false;
		}

		// attributes
		if(is_array($pa_data["attributes"]) && sizeof($pa_data["attributes"])) {
			foreach($pa_data["attributes"] as $vs_attribute_name => $va_values) {
				foreach($va_values as $va_value) {
					if($va_value["locale"]) {
						$va_value["locale_id"] = $t_locales->localeCodeToID($va_value["locale"]);
						unset($va_value["locale"]);
					} else {
						// use the default locale
						$va_value["locale_id"] = ca_locales::getDefaultCataloguingLocaleID();
					}

					$valueSource = '';

					if (isset($va_value['_value_source'])) {
						$valueSource = $va_value['_value_source'];
						unset($va_value['_value_source']);
					}

					$t_instance->addAttribute($va_value,$vs_attribute_name,null,null,$valueSource);
				}
			}
		}

		// VHH - add autogeneration of idno
		if (!empty($_GET["forceidno"]) && $_GET["forceidno"] === '1') {
			$o_numbering_plugin = $t_instance->getIDNoPlugInInstance();
			if (!($vs_sep = $o_numbering_plugin->getSeparator())) { $vs_sep = ''; }
			if (!is_array($va_idno_values = $o_numbering_plugin->htmlFormValuesAsArray('idno', null, false, false, true))) { $va_idno_values = array(); }
			$vs_idno_value = join($vs_sep, $va_idno_values);
			$t_instance->set('idno', $vs_idno_value);
		}

		$t_instance->setMode(ACCESS_WRITE);
		$t_instance->insert();

		if(!$t_instance->getPrimaryKey()) {
			$this->opa_errors = array_merge($t_instance->getErrors(),$this->opa_errors);
			return false;
		}

		// AFTER INSERT STUFF

		// preferred labels
		if(is_array($pa_data["preferred_labels"]) && sizeof($pa_data["preferred_labels"])) {
			foreach($pa_data["preferred_labels"] as $va_label) {
				if($va_label["locale"]) {
					$vn_locale_id = $t_locales->localeCodeToID($va_label["locale"]);
					unset($va_label["locale"]);
				} else {
					// use the default locale
					$vn_locale_id = ca_locales::getDefaultCataloguingLocaleID();
				}

				$t_instance->addLabel($va_label,$vn_locale_id,null,true);
			}
		}

		if(($t_instance instanceof LabelableBaseModelWithAttributes) && !$t_instance->getPreferredLabelCount()) {
			$t_instance->addDefaultLabel();
		}

		// nonpreferred labels
		if(is_array($pa_data["nonpreferred_labels"]) && sizeof($pa_data["nonpreferred_labels"])) {
			foreach($pa_data["nonpreferred_labels"] as $va_label) {
				if($va_label["locale"]) {
					$vn_locale_id = $t_locales->localeCodeToID($va_label["locale"]);
					unset($va_label["locale"]);
				} else {
					// use the default locale
					$vn_locale_id = ca_locales::getDefaultCataloguingLocaleID();
				}
				if($va_label["type_id"]) {
					$vn_type_id = $va_label["type_id"];
					unset($va_label["type_id"]);
				} else {
					$vn_type_id = null;
				}
				$t_instance->addLabel($va_label,$vn_locale_id,$vn_type_id,false);
			}
		}

		// relationships
		if(is_array($pa_data["related"]) && sizeof($pa_data["related"])>0) {
			foreach($pa_data["related"] as $vs_table => $va_relationships) {
				if($vs_table == 'ca_sets') {
					foreach($va_relationships as $va_relationship) {
						$t_set = new ca_sets();
						if ($t_set->load($va_relationship)) {
							$t_set->addItem($t_instance->getPrimaryKey());
						}
					}
				} else {
					foreach($va_relationships as $va_relationship) {
						$vs_source_info = isset($va_relationship["source_info"]) ? $va_relationship["source_info"] : null;
						$vs_effective_date = isset($va_relationship["effective_date"]) ? $va_relationship["effective_date"] : null;
						$vs_direction = isset($va_relationship["direction"]) ? $va_relationship["direction"] : null;

						$t_rel_instance = $this->_getTableInstance($vs_table);

						$vs_pk = isset($va_relationship[$t_rel_instance->primaryKey()]) ? $va_relationship[$t_rel_instance->primaryKey()] : null;
						$vs_type_id = isset($va_relationship["type_id"]) ? $va_relationship["type_id"] : null;

						$t_rel = $t_instance->addRelationship($vs_table,$vs_pk,$vs_type_id,$vs_effective_date,$vs_source_info,$vs_direction);

						// deal with interstitial attributes
						if($t_rel instanceof BaseRelationshipModel) {

							$vb_have_to_update = false;
							if(is_array($va_relationship["attributes"]) && sizeof($va_relationship["attributes"])) {
								foreach($va_relationship["attributes"] as $vs_attribute_name => $va_values) {
									foreach($va_values as $va_value) {
										if($va_value["locale"]) {
											$va_value["locale_id"] = $t_locales->localeCodeToID($va_value["locale"]);
											unset($va_value["locale"]);
										} else {
											// use the default locale
											$va_value["locale_id"] = ca_locales::getDefaultCataloguingLocaleID();
										}
										// VHH CHANGES - START
										// Added value source to interstitial records
										$valueSource = '';

										if (isset($va_value['_value_source'])) {
											$valueSource = $va_value['_value_source'];
											unset($va_value['_value_source']);
										}

										$t_rel->addAttribute($va_value,$vs_attribute_name,null,null,$valueSource);
										// VHH CHANGES - END

										$vb_have_to_update = true;
									}
								}
							}

							if($vb_have_to_update) {
								$t_rel->setMode(ACCESS_WRITE);
								$t_rel->update();
							}
						}
					}
				}
			}

			if(($t_instance instanceof RepresentableBaseModel) && isset($pa_data['representations']) && is_array($pa_data['representations'])) {
				foreach($pa_data['representations'] as $va_rep) {
					if(!isset($va_rep['media']) || (!file_exists($va_rep['media']) && !isURL($va_rep['media']))) { continue; }

					if(!($vn_rep_locale_id = $t_locales->localeCodeToID($va_rep['locale']))) {
						$vn_rep_locale_id = ca_locales::getDefaultCataloguingLocaleID();
					}

					$t_instance->addRepresentation(
						$va_rep['media'],
						caGetOption('type', $va_rep, 'front'), // this might be retarded but most people don't change the representation type list
						$vn_rep_locale_id,
						caGetOption('status', $va_rep, 0),
						caGetOption('access', $va_rep, 0),
						(bool)$va_rep['primary'],
						is_array($va_rep['values']) ? $va_rep['values'] : null
					);
				}
			}
		}
		
        if(($ps_table == 'ca_sets') && is_array($pa_data["set_content"]) && sizeof($pa_data["set_content"])>0) {
            $vn_table_num = $t_instance->get('table_num');
            if($t_set_table =  Datamodel::getInstance($vn_table_num)) {
                $vs_set_table = $t_set_table->tableName();
                foreach($pa_data["set_content"] as $vs_idno) {
                    if ($vn_set_item_id = $vs_set_table::find(['idno' => $vs_idno], ['returnAs' => 'firstId'])) {
                        $t_instance->addItem($vn_set_item_id);
                    }
                }
            }
        }
        
        // Set ACL for newly created record
		if ($t_instance->getAppConfig()->get('perform_item_level_access_checking') && !$t_instance->getAppConfig()->get("{$ps_table}_dont_do_item_level_access_control")) {
			$t_instance->setACLUsers(array($this->opo_request->getUserID() => __CA_ACL_EDIT_DELETE_ACCESS__));
			$t_instance->setACLWorldAccess($t_instance->getAppConfig()->get('default_item_access_level'));
		}

		if($t_instance->numErrors()>0) {
			foreach($t_instance->getErrors() as $vs_error) {
				$this->addError($vs_error);
			}
			// don't leave orphaned record in case something
			// went wrong with labels or relationships
			if($t_instance->getPrimaryKey()) {
				$t_instance->delete();
			}
			return false;
		} else {
			return array($t_instance->primaryKey() => $t_instance->getPrimaryKey());
		}
	}
	# -------------------------------------------------------
  private function uploadThumb($ps_table=null) {
		if(!$ps_table) { $ps_table = $this->ops_table; }
		
		if(!($t_instance = $this->_getTableInstance($this->ops_table,$this->opn_id))) {
			$this->addError('instance_not_found');
			return false;
		}

		// Check if file data is present
		if (empty($_FILES) || empty($_FILES['thumb']) || empty($_FILES['thumb']['tmp_name'])) {
			$this->addError('missing_file_data');
			return false;
		}

    $va_options = [
      'original_filename' => $_FILES['thumb']['name'],
      'returnRepresentation' => true
    ];

    if ($ps_table !== "ca_objecs") {
      $va_options["type_id"] = $this->ENTITY_REPRESENTATION_CREATOR_TYPE_ID;
    }

    $values = null;

    if (!empty($_POST['name'])) {
      $values = [
        "name" => $_POST['name']
      ];
    }

		// Create new secondary Representation
		$t_rep = $t_instance->addRepresentation(
			$_FILES['thumb']['tmp_name'],
      caGetOption('type', [], 'thumbnail'), // thumbnail seems to be a custom type in VHH
			ca_locales::getDefaultCataloguingLocaleID(),
			0,				// Status
			1,				// Access Status
			false,			// Primary
			$values,	// values
			$va_options
		);

		if($t_instance->numErrors()>0) {
			foreach($t_instance->getErrors() as $vs_error) {
				$this->addError($vs_error);
			}
			return false;
		}

    if (empty($t_rep)) {
      return false;
    }

		$va_removed_ids = array();
		$va_new_info = array();

		$o_service_config = Configuration::load(__CA_APP_DIR__."/conf/services.conf");
		$va_versions = $o_service_config->get('item_service_media_versions');
		if(!is_array($va_versions) || !sizeof($va_versions)) {
			$va_versions = ['preview170','large','original'];
		}

		// Add info on thumbnail representation and delete all other thumbnail representations
		if (is_array($va_reps = $t_instance->getRepresentations($va_versions))) {
			foreach ($va_reps as $vn_i => $va_rep_info) {
				if ($va_rep_info['type_id'] == $this->THUMBNAIL_TYPE_ID) {
          if ($va_rep_info['representation_id'] == ''.$t_rep->getPrimaryKey())
          {
            $va_new_info = $va_rep_info;
          } else {
            $t_instance->removeRepresentation($va_rep_info['representation_id']);
            $va_removed_ids[] = $va_rep_info['representation_id'];
          }
				}
			}
		}
		
		return array('new_representation' => $va_new_info, 'removed_ids' => $va_removed_ids);
	}
	# -------------------------------------------------------
  private function deleteThumb($ps_table=null) {
		if(!$ps_table) { $ps_table = $this->ops_table; }
		
		if(!($t_instance = $this->_getTableInstance($this->ops_table,$this->opn_id))) {
			$this->addError('instance_not_found');
			return false;
		}

		$vs_removed_id = '';

		// Add info on primary representation and delete all other representations that are not primary
		if (is_array($va_reps = $t_instance->getRepresentations($va_versions))) {
			foreach ($va_reps as $vn_i => $va_rep_info) {
        if ($va_rep_info['type_id'] == $this->THUMBNAIL_TYPE_ID) {
					$vs_removed_id = $va_rep_info['representation_id'];
					$t_instance->removeRepresentation($va_rep_info['representation_id']);
				}
			}
		}
		
		return array('removed_id' => $vs_removed_id);
	}
	# -------------------------------------------------------
	private function editItem($ps_table=null) {
		if(!$ps_table) { $ps_table = $this->ops_table; }
		if(!($t_instance = $this->_getTableInstance($this->ops_table,$this->opn_id))) {
			return false;
		}

		$t_locales = new ca_locales();
		$va_post = $this->getRequestBodyArray();

		// intrinsic fields
		if(is_array($va_post["intrinsic_fields"]) && sizeof($va_post["intrinsic_fields"])) {
			foreach($va_post["intrinsic_fields"] as $vs_field_name => $vs_value) {
				$t_instance->set($vs_field_name,$vs_value);
			}
		}

		// VHH CHANGES - START
		if(is_array($va_post["remove_attributes_by_id"])) {
			foreach($va_post["remove_attributes_by_id"] as $vs_id_to_delete) {
				$t_instance->removeAttribute($vs_id_to_delete);
			}
		}
		// attributes
		else if(is_array($va_post["remove_attributes"])) {
		// VHH CHANGES - END
			foreach($va_post["remove_attributes"] as $vs_code_to_delete) {
				$t_instance->removeAttributes($vs_code_to_delete);
			}
		} else if ($va_post["remove_all_attributes"]) {
			$t_instance->removeAttributes();
		}

		if(is_array($va_post["attributes"]) && sizeof($va_post["attributes"])) {
			foreach($va_post["attributes"] as $vs_attribute_name => $va_values) {
				foreach($va_values as $va_value) {
					if($va_value["locale"]) {
						$va_value["locale_id"] = $t_locales->localeCodeToID($va_value["locale"]);
						unset($va_value["locale"]);
					} else {
						// use the default locale
						$va_value["locale_id"] = ca_locales::getDefaultCataloguingLocaleID();
					}

					$valueSource = '';

					if (isset($va_value['_value_source'])) {
						$valueSource = $va_value['_value_source'];
						unset($va_value['_value_source']);
					}

					$t_instance->addAttribute($va_value,$vs_attribute_name,null,null,$valueSource);
				}
			}
		}

		// VHH CHANGES - START
		if(is_array($va_post["update_attributes"]) && sizeof($va_post["update_attributes"])) {
			foreach($va_post["update_attributes"] as $vs_attribute_name => $va_values) {
				foreach($va_values as $va_value) {
					if (!empty($va_value['_id'])) {
						$vs_attribute_id = $va_value['_id'];
						unset($va_value['_id']);

						if (isset($va_value['_value_source'])) {
							$o_db = new Db();
							$o_db->query("UPDATE ca_attributes SET value_source=\"".$va_value['_value_source']."\" WHERE attribute_id = ".$vs_attribute_id, []);
							unset($va_value['_value_source']);
						}

						if($va_value["locale"]) {
							$va_value["locale_id"] = $t_locales->localeCodeToID($va_value["locale"]);
							unset($va_value["locale"]);
						} else {
							// use the default locale
							$va_value["locale_id"] = ca_locales::getDefaultCataloguingLocaleID();
						}

						$t_instance->editAttribute($vs_attribute_id, $vs_attribute_name, $va_value);
					}
				}
			}
		}
		// VHH CHANGES - END

		$errors_so_far = $t_instance->getErrors();

		$t_instance->setMode(ACCESS_WRITE);
		$t_instance->update();

		// AFTER UPDATE STUFF

		// yank all labels?
		if ($va_post["remove_all_labels"]) {
			$t_instance->removeAllLabels();
		}

		// preferred labels
		if(is_array($va_post["preferred_labels"]) && sizeof($va_post["preferred_labels"])) {
			foreach($va_post["preferred_labels"] as $va_label) {
				if($va_label["locale"]) {
					$vn_locale_id = $t_locales->localeCodeToID($va_label["locale"]);
					unset($va_label["locale"]);
				} else {
					// use the default locale
					$vn_locale_id = ca_locales::getDefaultCataloguingLocaleID();
				}
				$t_instance->addLabel($va_label,$vn_locale_id,null,true);
			}
		}

		// nonpreferred labels
		if(is_array($va_post["nonpreferred_labels"]) && sizeof($va_post["nonpreferred_labels"])) {
			foreach($va_post["nonpreferred_labels"] as $va_label) {
				if($va_label["locale"]) {
					$vn_locale_id = $t_locales->localeCodeToID($va_label["locale"]);
					unset($va_label["locale"]);
				} else {
					// use the default locale
					$vn_locale_id = ca_locales::getDefaultCataloguingLocaleID();
				}
				if($va_label["type_id"]) {
					$vn_type_id = $va_label["type_id"];
					unset($va_label["type_id"]);
				} else {
					$vn_type_id = null;
				}
				$t_instance->addLabel($va_label,$vn_locale_id,$vn_type_id,false);
			}
		}

		// relationships
		if (is_array($va_post["remove_relationships"])) {
			foreach($va_post["remove_relationships"] as $vs_table) {
				$t_instance->removeRelationships($vs_table);
			}
		}

		if($va_post["remove_all_relationships"]) {
			foreach($this->opa_valid_tables as $vs_table) {
				$t_instance->removeRelationships($vs_table);
			}
		}

    // VHH - START
		if($va_post["remove_relationships_by_id"]) {
			foreach($va_post["remove_relationships_by_id"] as $vs_table => $va_type_ids) {
				foreach($va_type_ids as $vs_type_id) {
					$t_instance->removeRelationship($vs_table, intval($vs_type_id));
				}
			}
		}
    // VHH - END

    // VHH - START
		// Update relationship type for existing relations
		if(is_array($va_post["update_relationship_types"]) && sizeof($va_post["update_relationship_types"])) {
			foreach($va_post["update_relationship_types"] as $vs_table => $va_relationship_info) {
				foreach ($va_relationship_info as $va_relationship) {
					if (!empty($va_relationship['direction']) && $va_relationship['direction'] == 'rtol') {
						$vs_direction = 'rtol';
					} else {
						$vs_direction = 'ltor';
					}
					$t_instance->editRelationship($vs_table, $va_relationship['relation_id'], $va_relationship['rel_id'], $va_relationship['type_id'], null, null, $vs_direction);
				}
			}
		}
    // VHH - END

		// VHH - START
		// Add interstitial record
		// Update existing interstitial record
		if(is_array($va_post["add_interstitial"]) && sizeof($va_post["add_interstitial"])) {
			foreach($va_post["add_interstitial"] as $vs_table => $va_relationship_info) {
				if ($t_rel_instance = $t_instance->getRelationshipInstance($vs_table)) {
					foreach ($va_relationship_info as $va_relationship) {
						$t_rel_instance->load($va_relationship['relation_id']);

						foreach ($va_relationship['attrs'] as $vs_attribute_name => $va_values) {
							foreach($va_values as $va_value) {
								$valueSource = '';

								if (isset($va_value['_value_source'])) {
									$valueSource = $va_value['_value_source'];
									unset($va_value['_value_source']);
								}

								$va_value["locale_id"] = ca_locales::getDefaultCataloguingLocaleID();
								$t_rel_instance->addAttribute($va_value,$vs_attribute_name,null,null,$valueSource);
							}
						}
						$t_rel_instance->setMode(ACCESS_WRITE);
						$t_rel_instance->update();
					}
				}
			}
		}

		// Update existing interstitial record
		if(is_array($va_post["update_interstitial"]) && sizeof($va_post["update_interstitial"])) {
			foreach($va_post["update_interstitial"] as $vs_table => $va_relationship_info) {
				if ($t_rel_instance = $t_instance->getRelationshipInstance($vs_table)) {
					foreach ($va_relationship_info as $va_relationship) {
						$t_rel_instance->load($va_relationship['relation_id']);

						foreach ($va_relationship['attrs'] as $vs_attribute_name => $va_values) {
							foreach($va_values as $va_value) {
								if (!empty($va_value['_id'])) {
									$vs_attribute_id = $va_value['_id'];
									unset($va_value['_id']);

									if (isset($va_value['_value_source'])) {
										$o_db = new Db();
										$o_db->query("UPDATE ca_attributes SET value_source=\"".$va_value['_value_source']."\" WHERE attribute_id = ".$vs_attribute_id, []);
										unset($va_value['_value_source']);
									}

									$va_value["locale_id"] = ca_locales::getDefaultCataloguingLocaleID();
									$t_rel_instance->editAttribute($vs_attribute_id, $vs_attribute_name, $va_value);
							  }
							}
						}
						$t_rel_instance->setMode(ACCESS_WRITE);
						$t_rel_instance->update();
					}
				}
			}
		}

		// Delete interstitial records from relationships
		if(is_array($va_post["delete_interstitial"]) && sizeof($va_post["delete_interstitial"])) {
			foreach($va_post["delete_interstitial"] as $vs_table => $va_relationship_info) {
				if ($t_rel_instance = $t_instance->getRelationshipInstance($vs_table)) {
					foreach ($va_relationship_info as $va_relationship) {
						$t_rel_instance->load($va_relationship['relation_id']);
						$t_rel_instance->removeAttribute($va_relationship['interstitial_id']);
						$t_rel_instance->setMode(ACCESS_WRITE);
						$t_rel_instance->update();
					}
				}
			}
		}
		// VHH - END

		if(is_array($va_post["related"]) && sizeof($va_post["related"])>0) {
			foreach($va_post["related"] as $vs_table => $va_relationships) {
				foreach($va_relationships as $va_relationship) {
					$vs_source_info = isset($va_relationship["source_info"]) ? $va_relationship["source_info"] : null;
					$vs_effective_date = isset($va_relationship["effective_date"]) ? $va_relationship["effective_date"] : null;
					$vs_direction = isset($va_relationship["direction"]) ? $va_relationship["direction"] : null;

					$t_rel_instance = $this->_getTableInstance($vs_table);

					$vs_pk = isset($va_relationship[$t_rel_instance->primaryKey()]) ? $va_relationship[$t_rel_instance->primaryKey()] : null;
					$vs_type_id = isset($va_relationship["type_id"]) ? $va_relationship["type_id"] : null;

					$t_rel = $t_instance->addRelationship($vs_table,$vs_pk,$vs_type_id,$vs_effective_date,$vs_source_info,$vs_direction);

					// deal with interstitial attributes
					if($t_rel instanceof BaseRelationshipModel) {

						$vb_have_to_update = false;
						if(is_array($va_relationship["attributes"]) && sizeof($va_relationship["attributes"])) {
							foreach($va_relationship["attributes"] as $vs_attribute_name => $va_values) {
								foreach($va_values as $va_value) {
									if($va_value["locale"]) {
										$va_value["locale_id"] = $t_locales->localeCodeToID($va_value["locale"]);
										unset($va_value["locale"]);
									} else {
										// use the default locale
										$va_value["locale_id"] = ca_locales::getDefaultCataloguingLocaleID();
									}
									// VHH CHANGES - START
									// Added value source to interstitial records
									$valueSource = '';

									if (isset($va_value['_value_source'])) {
										$valueSource = $va_value['_value_source'];
										unset($va_value['_value_source']);
									}

									$t_rel->addAttribute($va_value,$vs_attribute_name,null,null,$valueSource);
									// VHH CHANGES - END

									$vb_have_to_update = true;
								}
							}
						}

						if($vb_have_to_update) {
							$t_rel->setMode(ACCESS_WRITE);
							$t_rel->update();
						}
					}
				}
			}
		}
		
		if(($ps_table == 'ca_sets') && is_array($va_post["set_content"]) && sizeof($va_post["set_content"])>0) {
            $vn_table_num = $t_instance->get('table_num');
            if($t_set_table =  Datamodel::getInstance($vn_table_num)) {
                $vs_set_table = $t_set_table->tableName();
                
               $va_current_set_item_ids = $t_instance->getItems(['returnRowIdsOnly' => true]);
                foreach($va_post["set_content"] as $vs_idno) {
                    if (
                        ($vn_set_item_id = $vs_set_table::find(['idno' => $vs_idno], ['returnAs' => 'firstId']))
                        &&
                        (!$t_instance->isInSet($vs_set_table, $vn_set_item_id, $t_instance->getPrimaryKey()))
                    ) {
                        $t_instance->addItem($vn_set_item_id);
                    }
                    if ($vn_set_item_id) { unset($va_current_set_item_ids[$vn_set_item_id]); }
                }
                
                foreach(array_keys($va_current_set_item_ids) as $vn_item_id) {
                    $t_instance->removeItem($vn_item_id);
                }
            }
        }

		// if($t_instance->numErrors()>0) {
		if (count($errors_so_far) > 0) {
			foreach($errors_so_far as $vs_error) {
				$this->addError($vs_error);
			}
			return false;
		} else {
			return array($t_instance->primaryKey() => $t_instance->getPrimaryKey());
		}

	}
	# -------------------------------------------------------
	private function deleteItem() {
		if(!($t_instance = $this->_getTableInstance($this->ops_table,$this->opn_id))) {
			return false;
		}

		$va_post = $this->getRequestBodyArray();

		$vb_delete_related = isset($va_post["delete_related"]) ? $va_post["delete_related"] : false;
		$vb_hard_delete = isset($va_post["hard"]) ? $va_post["hard"] : false;

		$t_instance->setMode(ACCESS_WRITE);
		$t_instance->delete($vb_delete_related,array("hard" => $vb_hard_delete));


		if($t_instance->numErrors()>0) {
			foreach($t_instance->getErrors() as $vs_error) {
				$this->addError($vs_error);
			}
			return false;
		} else {
			return array("deleted" => $this->opn_id);
		}
	}
	# -------------------------------------------------------
}
