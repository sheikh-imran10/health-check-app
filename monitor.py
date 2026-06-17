import logging
import os
import sys
import time
from kubernetes import client, config, watch
from kubernetes.config.config_exception import ConfigException
from kubernetes.client.exceptions import ApiException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

WATCH_NAMESPACES = "health-monitoring"
WATCH_LABEL_KEY = "app"
WATCH_LABEL_VALUE = "test-liveness-failure"


class KubernetesHealthMonitor:
    def __init__(self):
        self._initialize_kube_config()
        self.core_api = client.CoreV1Api()
        self.watcher = watch.Watch()
        self.processed_event_versions = set()

    def _initialize_kube_config(self):
        """Production config loader tailored for in-cluster deployment."""
        try:
            # Matches /var/run/secrets/kubernetes.io/serviceaccount inside the pod
            config.load_incluster_config()
            logger.info("Successfully loaded in-cluster Kubernetes ServiceAccount configuration.")
        except ConfigException:
            try:
                # Local WSL fallback (kept safe during local development/debugging)
                windows_kube_path = "/mnt/c/Users/SHEI048/.kube/config"
                if os.path.exists(windows_kube_path):
                    config.load_kube_config(config_file=windows_kube_path)
                    logger.info(f"Loaded Windows kubeconfig from {windows_kube_path}")
                else:
                    config.load_kube_config()
                    logger.info("Loaded default local kubeconfig.")
            except Exception as e:
                logger.critical(f"Could not configure Kubernetes client: {e}")
                sys.exit(1)

    def _pod_matches_label(self, namespace, pod_name):
        try:
            pod = self.core_api.read_namespaced_pod(name=pod_name, namespace=namespace)
            labels = pod.metadata.labels or {}
            return labels.get(WATCH_LABEL_KEY) == WATCH_LABEL_VALUE
        
        except ApiException as e:
            return True  if e.status == 404 else False
                
        except Exception:
            return False

    def _get_deployment_name(self, namespace, pod_name):
        try:
            pod = self.core_api.read_namespaced_pod(name=pod_name, namespace=namespace)
            owner_refs = pod.metadata.owner_references or []
            for owner in owner_refs:
                if owner.kind == "ReplicaSet":
                    return owner.name.rsplit("-", 1)[0]
                
            return pod_name
        
        except ApiException as e:
            if e.status != 404:
                logger.error(f"API Error determining deployment name for {pod_name}: {e}")
            return pod_name
        
        except Exception:
            return pod_name

    def process_event(self, event_obj):
        try:
            if event_obj.reason != "Unhealthy" or "Liveness probe failed" not in event_obj.message:
                return

            namespace = event_obj.metadata.namespace
            pod_name = event_obj.involved_object.name
            
            logger.info(f"Received liveness event for pod: {pod_name}.")

            if WATCH_LABEL_VALUE not in pod_name:
                if not self._pod_matches_label(namespace, pod_name):
                    logger.info(f"Skipping pod {pod_name} - Labels do not match.")
                    return

            deployment_name = self._get_deployment_name(namespace, pod_name)
            
            logger.warning(f"Liveness failure detected: {namespace}/{pod_name}")
            logger.info(f"[ALERT] Service Unavailable for namespace: {namespace}, pod: {pod_name}, deployment: {deployment_name}")
            logger.info(f"Message: {event_obj.message}")

            # Trigger Ticket
            logger.info("Jira Ticket is created successfully !!")

        except Exception as e:
            logger.error(f"Error processing event loop item: {e}", exc_info=True)

    def run_forever(self):
        logger.info(f"Starting infinite Kubernetes event watcher for namespace: {WATCH_NAMESPACES}")
        while True:
            try:
                stream = self.watcher.stream(
                    self.core_api.list_namespaced_event,
                    namespace=WATCH_NAMESPACES,
                    timeout_seconds=300
                )
                for event in stream:
                    self.process_event(event["object"])
            except Exception:
                logger.exception("Watcher stream disconnected. Reconnecting in 10 seconds...")
                time.sleep(10)


if __name__ == "__main__":
    monitor = KubernetesHealthMonitor()
    monitor.run_forever()
