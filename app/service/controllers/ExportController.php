<?php
/* ----------------------------------------------------------------------
 * app/service/controllers/TimelineController.php :
 * ----------------------------------------------------------------------
 * VHH
 * Collect data the MMSI timeline
 * ----------------------------------------------------------------------
*/

require_once(__CA_LIB_DIR__.'/Service/BaseServiceController.php');
require_once(__CA_LIB_DIR__.'/Service/ExportService.php');

class ExportController extends BaseServiceController {
  # -------------------------------------------------------
  public function __construct(&$po_request, &$po_response, $pa_view_paths) {
    parent::__construct($po_request, $po_response, $pa_view_paths);
  }
 # -------------------------------------------------------
  public function __call($ps_object_type, $pa_args) {
    if ($this->opo_request->getRequestMethod() != 'GET') {
      $this->view->setVar("errors", array("Invalid HTTP request type"));
      $this->render("json_error.php");
      return;
    }

    $exportType = $this->opo_request->getParameter("type", pString);

    if ($exportType == 'nonav') {
      if ($ps_object_type != 'ca_objects') {
        $this->view->setVar("errors", array("NonAV export only works with object type ca_objects. Object type given: ".$ps_object_type));
        $this->render("json_error.php");
        return;
      }

      $vs_query = $this->opo_request->getParameter("query", pString);
      $vb_collect_images = $this->opo_request->getParameter("collect_images", pString) == '1';
      $vb_collect_meta = $this->opo_request->getParameter("collect_meta", pString) == '1';
      $vb_show_json = $this->opo_request->getParameter("show_json", pString) == '1';
      $va_content = ExportService::dispatchNonAv($vs_query, $vb_collect_images, $vb_collect_meta, $vb_show_json);
    } else if ($exportType == 'av') { 
      if ($ps_object_type != 'ca_objects') {
        $this->view->setVar("errors", array("NonAV export only works with object type ca_objects. Object type given: ".$ps_object_type));
        $this->render("json_error.php");
        return;
      }

      $vs_query = $this->opo_request->getParameter("query", pString);
      $vb_show_json = $this->opo_request->getParameter("show_json", pString) == '1';
      $va_content = ExportService::dispatchAv($vs_query, $vb_show_json);
    } else {
      if (!in_array($ps_object_type, ['ca_objects', 'ca_entities', 'ca_places', 'ca_occurrences', 'ca_collections'])) {
        $this->view->setVar("errors", array("Unknown object type: ".$ps_object_type));
        $this->render("json_error.php");
        return;
      }

      $vs_query = $this->opo_request->getParameter("query", pString);
      $vb_show_json = $this->opo_request->getParameter("show_json", pString) == '1';
      $va_content = ExportService::dispatchFlat($ps_object_type, $vs_query, $vb_show_json);
    }
    
    if(intval($this->request->getParameter("pretty", pInteger)) > 0){
      $this->view->setVar("pretty_print",true);
    }
    
    $this->view->setVar("content",$va_content);
    $this->render("json.php");
  }
}