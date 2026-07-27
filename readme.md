# HPA/KEDA Assessment for Kafka Consumer Applications

**Cluster:** flux-asys-nmda-sdms-apim-test01 (TEST)  
**Namespace:** itsm-apps  
**Generated:** 2026-07-27

## Executive Summary

This document provides an assessment of all Kafka consumer applications in the ITSM Service Bus (ITSMSB) for implementing HPA (Horizontal Pod Autoscaler) or KEDA (Kubernetes Event-Driven Autoscaler) based scaling.

## Assessment Spreadsheet

| Deployment | Topic(s) | Partitions | Consumer Group | Consumer Threads per Pod | Message Ordering Required | Idempotent Processing | Current Replicas | Recommended maxReplicaCount | Scaling Metric | Risk Level |
|------------|----------|------------|----------------|-------------------------|---------------------------|----------------------|------------------|------------------------------|----------------|------------|
| asy-sm-pusher | incident_to_ewe_ot, wwn-usm-pusher, sco-servicenow-pusher, confirmation-to-jira-case-exchange | 3 (est.) | asy-sm-pusher | 1 | Per key (Ticket ID) | Yes | 5 (int) | 3 | Kafka consumer lag | Medium |
| asy-jira-ticket-pusher | asy-jira-ticket-pusher | 3 (est.) | asy-jira-ticket-pusher | 1 | Per key (Ticket ID) | Yes | 5 (dev), 1 (int) | 5 | Kafka consumer lag | Low |
| asy-jira-asset-pusher | asy-jira-asset-pusher | 3 (est.) | asy-jira-asset-pusher | 1 | No | Yes | 1 | 3 | Kafka consumer lag | Low |
| asy-jira-organization-pusher | asy-jira-organization-pusher | 3 (est.) | asy-jira-organization-pusher | 1 | Per key (Org ID) | Yes | 1 | 3 | Kafka consumer lag | Low |
| asy-jira-asset-live-pusher | asy-jira-asset-live-pusher | 3 (est.) | asy-jira-asset-live-pusher | 1 | No | Yes | 1 | 3 | Kafka consumer lag | Low |
| asy-ucmdb-jira-asset-live-pusher | asy-ucmdb-jira-asset-live-pusher | 3 (est.) | asy-ucmdb-jira-asset-live-pusher | 1 | No | Yes | 1 | 3 | Kafka consumer lag | Low |
| ewe-ot-pusher | incident_to_ewe_ot | 3 (est.) | ewe-ot-pusher | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Medium |
| wwn-usm-pusher | wwn-usm-pusher | 3 (est.) | wwn-usm-pusher | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Medium |
| sco-servicenow-pusher | sco-servicenow-pusher | 3 (est.) | sco-servicenow-pusher | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Medium |
| kub-esb-pusher-py | update_node_to_kub | 3 (est.) | kub-esb-pusher-py | 1 | Per key (Node ID) | Yes | 1 | 3 | Kafka consumer lag | Low |
| kub-zis-event-pusher | kub-zis-event-pusher | 3 (est.) | kub-zis-event-pusher | 1 | Per key (Event ID) | Yes | 1 | 3 | Kafka consumer lag | Low |
| tnt-topdesk-pusher | tnt-topdesk-pusher-v2 | 3 (est.) | tnt-topdesk-pusher | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Medium |
| hnb-m42-pusher | haver-and-boecker-matrix-pusher | 3 (est.) | hnb-m42-pusher | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Medium |
| asy-obm-event-pusher | event_to_asy_obm | 3 (est.) | asy-obm-event-pusher | 1 | Per key (Event ID) | Yes | 1 | 3 | Kafka consumer lag | Low |
| asy-obm-event-updater | asy-obm-event-updater | 3 (est.) | asy-obm-event-updater | 1 | Per key (Event ID) | Yes | 1 | 3 | Kafka consumer lag | Low |
| asy-jsm-ticket-sender | asy-jsm-ticket-sender | 3 (est.) | asy-jsm-ticket-sender | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Medium |
| asy-github-backup-automation-rules-pusher | asy-github-backup-automation-rules | 3 (est.) | asy-github-backup | 1 | No | Yes | 1 | 2 | Kafka consumer lag | Low |
| asy-azure-backup-automation-rules-pusher | asy-azure-backup-automation-rules | 3 (est.) | asy-azure-backup | 1 | No | Yes | 1 | 2 | Kafka consumer lag | Low |
| asy-streamworks-pusher | asy-streamworks-pusher | 3 (est.) | asy-streamworks-pusher | 1 | No | Yes | 1 | 3 | Kafka consumer lag | Low |
| oge-jsm-ticket-pusher | oge-jsm-ticket-pusher | 3 (est.) | oge-jsm-ticket-pusher | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Medium |
| asy-varedy-ticket-pusher | asy-varedy-ticket-pusher | 3 (est.) | asy-varedy-ticket-pusher | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Low |
| asy-sap-cpi-order-sender | asy-sap-cpi-order-sender | 3 (est.) | asy-sap-cpi-order-sender | 1 | Per key (Order ID) | Yes | 1 | 3 | Kafka consumer lag | Medium |
| asy-jsm-ticket-distributor | asy-jsm-ticket-distributor | 3 (est.) | asy-jsm-ticket-distributor | 1 | Per key (Ticket ID) | Yes | 1 | 3 | Kafka consumer lag | Low |

## Column Definitions

| Field | Description |
|-------|-------------|
| **Deployment** | Name of the Kubernetes Deployment for the Kafka consumer |
| **Topic(s)** | Kafka topic(s) the consumer subscribes to |
| **Partitions** | Number of partitions for the topic (determines max parallelism). *Note: "est." = estimated; verify with Kafka admin* |
| **Consumer Group** | Kafka consumer group ID used by the application |
| **Consumer Threads per Pod** | Number of consumer threads running in each pod (typically 1 for Python apps) |
| **Message Ordering Required** | Whether message ordering must be preserved ("Per key" = ordering per partition key, "No" = no ordering requirement) |
| **Idempotent Processing** | Whether the consumer handles duplicate messages safely |
| **Current Replicas** | Current number of pod replicas configured |
| **Recommended maxReplicaCount** | Maximum replicas recommended (should not exceed partition count) |
| **Scaling Metric** | Primary metric for autoscaling decisions |
| **Risk Level** | Risk assessment for scaling (Low/Medium/High) |

## Risk Level Criteria

| Risk Level | Criteria |
|------------|----------|
| **Low** | Non-critical data sync, batch processing, asset imports. Can tolerate brief delays. |
| **Medium** | Ticket/event processing with SLA requirements. External system integrations. |
| **High** | Real-time alerting, critical incident handling. Requires immediate processing. |

## Recommendations

### 1. Immediate KEDA Implementation Candidates (Low Risk)

These consumers are good candidates for initial KEDA implementation:

- `asy-jira-asset-pusher` - Asset sync, can handle lag
- `asy-jira-organization-pusher` - Org sync, batch-like processing
- `asy-streamworks-pusher` - Data preparation workflow
- `asy-github-backup-automation-rules-pusher` - Backup process, non-critical timing
- `kub-esb-pusher-py` - CMDB sync to external system

### 2. Medium Risk - Careful Implementation

These require more careful testing due to external system dependencies:

- `asy-jira-ticket-pusher` - High volume, critical for ticket delivery
- `asy-sm-pusher` - Multiple destination topics, complex routing
- `ewe-ot-pusher` - External system (OmniTracker) integration
- `wwn-usm-pusher` - External system (USM) integration
- `sco-servicenow-pusher` - External system (ServiceNow) integration
- `tnt-topdesk-pusher` - External system (TOPdesk) integration

### 3. Pre-Implementation Checklist

Before implementing KEDA scaling:

- [ ] Verify actual partition count for each topic with Kafka admin
- [ ] Confirm consumer group names match deployment names (convention)
- [ ] Test idempotency of each consumer under duplicate message scenarios
- [ ] Verify external system rate limits can handle increased throughput
- [ ] Review Redis/database connection pooling for scaled pods
- [ ] Test with scale-to-zero if applicable (some consumers should always run)

### 4. Existing HPA

Currently, only `asy-sm-puller-deployment` has an HPA configured:
- **Min Replicas:** 1
- **Max Replicas:** 3
- **Metrics:** CPU (60%) and Memory (70%)
- **File:** [asy-sm-puller-deployment.hpa.yaml](../custom-install/itsm-apps/itsmsb/asy/sm/asy-sm-puller/asy-sm-puller-deployment.hpa.yaml)

### 5. KEDA ScaledObject Template

Example KEDA ScaledObject for Kafka consumer:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {deployment-name}-scaledobject
  namespace: itsm-apps
spec:
  scaleTargetRef:
    name: {deployment-name}
  pollingInterval: 30
  cooldownPeriod: 300
  minReplicaCount: 1
  maxReplicaCount: 3  # Should not exceed partition count
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: itsmsb-kafka-kafka-bootstrap:9092
        consumerGroup: {consumer-group}
        topic: {topic-name}
        lagThreshold: "100"  # Messages per partition
        offsetResetPolicy: latest
```

## Notes

1. **Partition Count:** All topics appear to use default partition count (estimated 3). Verify with Kafka cluster admin before implementing KEDA to ensure maxReplicaCount doesn't exceed partition count.

2. **Consumer Groups:** Most applications follow the pattern where the consumer group matches the deployment name. Verify in application code or logs.

3. **Rate Limiting:** External systems (ServiceNow, OmniTracker, USM, TOPdesk, M42) may have API rate limits. Scaling up consumers may hit these limits.

4. **Redis Dependencies:** Many pushers use Redis for message deduplication and state management. Ensure Redis can handle increased concurrent connections.

5. **Database Connections:** Applications using PostgreSQL (`itsmapps` database) should have connection pooling configured to handle scaled pods.
