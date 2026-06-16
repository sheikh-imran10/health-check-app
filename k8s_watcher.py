import logging
import time

from kubernetes import client, config, watch

logger = logging.getLogger(__name__)

WATCH_NAMESPACES = "itsm-apps"

WATCH_LABEL_KEY = ""
WATCH_LABEL_VALUE = ""


class KubernetesHealthMonitor:
    def __init__(self, obm_event_factory):
        self.obm_event_factory = obm_event_factory
        self.core_api = client.CoreV1Api()
        self.watcher = watch.Watch()
        config.load_incluster_config()

    def _pod_matches_label(self, namespace, pod_name):
        try:
            pod = self.core_api.read_namespaced_pod(
                name=pod_name,
                namespace=namespace
            )

            labels = pod.metadata.labels or {}

            return (
                labels.get(WATCH_LABEL_KEY)
                == WATCH_LABEL_VALUE
            )

        except Exception:
            logger.exception(
                "Failed to read pod %s/%s",
                namespace,
                pod_name
            )
            return False

    def _create_incident(self, namespace, pod_name, deployment_name, message):

        exception_message = (
            f"Liveness probe failed\n"
            f"Namespace: {namespace}\n"
            f"Deployment: {deployment_name}\n"
            f"Pod: {pod_name}\n"
            f"Message: {message}"
        )

        self.obm_event_factory.create_event_from_exception(
            exception=Exception(exception_message),
            send_event=True,
            log_exception=True,
            title_addon=(
                f" - Kubernetes Liveness Failure "
                f"({deployment_name})"
            ),
            relatedCiHint="ITSM Automation - Health Monitor"
        )

    def _get_deployment_name(self, namespace, pod_name):
        try:
            pod = self.core_api.read_namespaced_pod(
                name=pod_name,
                namespace=namespace
            )

            owner_refs = pod.metadata.owner_references or []

            for owner in owner_refs:
                # ReplicaSet -> deployment
                if owner.kind == "ReplicaSet":
                    rs_name = owner.name

                    # payment-service-7c4d5f68d7
                    deployment_name = rs_name.rsplit("-", 1)[0]

                    return deployment_name

            return pod_name

        except Exception:
            logger.exception(
                "Failed determining deployment name"
            )
            return pod_name

    def process_event(self, event_obj):
        try:
            if event_obj.reason != "Unhealthy":
                return

            if "Liveness probe failed" not in event_obj.message:
                return

            namespace = event_obj.metadata.namespace
            pod_name = event_obj.involved_object.name

            if not self._pod_matches_label(namespace, pod_name):
                return

            deployment_name = self._get_deployment_name(namespace, pod_name)

            logger.warning("Liveness failure detected: %s/%s",namespace, pod_name)

            self._create_incident(
                namespace=namespace,
                pod_name=pod_name,
                deployment_name=deployment_name,
                message=event_obj.message,
            )

        except Exception:
            logger.exception("Error processing Kubernetes event")

    def start(self):
        logger.info("Starting Kubernetes event watcher")

        while True:
            try:
                stream = self.watcher.stream(
                    self.core_api.list_namespaced_event,
                    namespace="itsm-apps",
                    timeout_seconds=300,
                )

                for event in stream:
                    self.process_event(event["object"])

            except Exception as e:
                logger.exception("Watcher disconnected. Reconnecting...")
                time.sleep(10)
