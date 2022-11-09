<?php
/* ----------------------------------------------------------------------
 * app/service/controllers/TimelineController.php :
 * ----------------------------------------------------------------------
 * VHH
 * Collect data the MMSI timeline
 * ----------------------------------------------------------------------
*/

require_once(__CA_LIB_DIR__.'/Service/BaseServiceController.php');
require_once(__CA_LIB_DIR__.'/Service/TimelineService.php');

class TimelineController extends BaseServiceController {
		# -------------------------------------------------------
		public function __construct(&$po_request, &$po_response, $pa_view_paths) {
 			parent::__construct($po_request, $po_response, $pa_view_paths);
 		}
		# -------------------------------------------------------
		public function __call($ps_timeline_type, $pa_args){
			if ($this->opo_request->getRequestMethod() == 'GET') {
				$vb_force_refresh = $this->opo_request->getParameter("force_refresh", pString) == '1';
				$va_content = TimelineService::dispatch($ps_timeline_type, $vb_force_refresh);

				if(intval($this->request->getParameter("pretty",pInteger))>0){
					$this->view->setVar("pretty_print",true);
				}
				
				$this->view->setVar("content",$va_content);
				$this->render("json.php");
			} else {
				$this->view->setVar("errors", array("Invalid HTTP request type"));
				$this->render("json_error.php");
			}
		}
}