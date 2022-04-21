<?php
	require_once(__CA_LIB_DIR__.'/Service/BaseServiceController.php');
	require_once(__CA_LIB_DIR__.'/Service/ManifestService.php');

	class ManifestController extends BaseServiceController {
		# -------------------------------------------------------
		public function __construct(&$po_request, &$po_response, $pa_view_paths) {
 			parent::__construct($po_request, $po_response, $pa_view_paths);
 		}
		# -------------------------------------------------------
		public function __call($ps_table, $pa_args){
			$vo_service = new ManifestService($this->request,$ps_table);
			$va_content = $vo_service->dispatch();

			if(intval($this->request->getParameter("pretty",pInteger))>0){
				$this->view->setVar("pretty_print",true);
			}
			
			if($vo_service->hasErrors()){
				$this->view->setVar("errors",$vo_service->getErrors());
				$this->render("json_error.php");
			} else {
				$this->view->setVar("content",$va_content);
				$this->render("json.php");
			}
		}
		# -------------------------------------------------------
	}
