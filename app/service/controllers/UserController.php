<?php
require_once(__CA_LIB_DIR__.'/Service/BaseServiceController.php');

# -------------------------------------------------------
# VHH: Added a me resource in order to get information about the logged in user
class UserController extends BaseServiceController {
	# -------------------------------------------------------
	public function __construct(&$po_request, &$po_response, $pa_view_paths) {
		parent::__construct($po_request, $po_response, $pa_view_paths);
	}
	# -------------------------------------------------------
	public function __call($ps_endpoint, $pa_args) {
		try {
			if ($ps_endpoint == 'me') {
				$vs_user_id = $this->request->getUserID();
				$t_user = new ca_users($vs_user_id);

				$this->view->setVar("content",array(
					'id' => $vs_user_id,
					'username' => $t_user->get('ca_users.user_name'),
					'name' => $t_user->get('ca_users.fname').' '.$t_user->get('ca_users.lname'),
					'email' => $t_user->get('ca_users.email')
				));
				$this->render('json.php');
			} else {
				$this->getView()->setVar('errors', array('invalid path'));
				$this->render('json_error.php');
				return;
			}
		} catch(Exception $e) {
			$this->getView()->setVar('errors', array($e->getMessage()));
			$this->render('json_error.php');
			return;
		}
	}
	# -------------------------------------------------------
}
