// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Bounded Prometheus projection of the node decision core.
//!
//! The operator endpoint deliberately exposes no cell, request, tenant, or
//! bucket identifiers. Every label has a runtime-owned bounded vocabulary
//! except `region` and the build version supplied by the operator binary.

use celld_logic::{
    pressure::{SHED_MEMORY, SHED_RSS_HARD},
    ActivitySnapshot, STABLE_PHASE_NAMES,
};
use std::fmt::Write;

pub struct NodeMetrics<'a> {
    pub runtime_version: &'a str,
    pub region: &'a str,
    pub ownership: &'a str,
    pub serving: bool,
    pub occupied: usize,
    pub resident_limit: Option<usize>,
    pub evicting: usize,
    pub restoring: usize,
    pub activating: usize,
    pub activation_waiting: usize,
    pub capacity_waiting: usize,
    pub phases: &'a [(&'static str, usize)],
    pub shed_reason: Option<&'static str>,
    pub rss_bytes: u64,
    pub in_use_bytes: u64,
    pub output_gate_pending: usize,
    pub publishes: u64,
    pub stops: u64,
    pub activity: ActivitySnapshot,
}

fn label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn help_and_type(output: &mut String, name: &str, help: &str, metric_type: &str) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {metric_type}");
}

pub fn render(metrics: &NodeMetrics<'_>) -> String {
    let mut output = String::with_capacity(4096);

    help_and_type(
        &mut output,
        "celld_build_info",
        "Build and deployment identity for this celld process.",
        "gauge",
    );
    let _ = writeln!(
        output,
        "celld_build_info{{version=\"{}\",region=\"{}\"}} 1",
        label_value(metrics.runtime_version),
        label_value(metrics.region),
    );

    help_and_type(
        &mut output,
        "celld_node_serving",
        "Whether the node decision core currently permits request service.",
        "gauge",
    );
    let _ = writeln!(output, "celld_node_serving {}", u8::from(metrics.serving));

    help_and_type(
        &mut output,
        "celld_node_ownership_state",
        "Current node ownership backend state.",
        "gauge",
    );
    let _ = writeln!(
        output,
        "celld_node_ownership_state{{state=\"{}\"}} 1",
        label_value(metrics.ownership),
    );

    for (name, help, value) in [
        (
            "celld_node_occupied_cells",
            "Resident cells plus activation reservations on this node.",
            metrics.occupied,
        ),
        (
            "celld_node_evicting_cells",
            "Cells with an ownership eviction in flight.",
            metrics.evicting,
        ),
        (
            "celld_node_restoring_cells",
            "Cold routes either activating or queued for activation.",
            metrics.restoring,
        ),
        (
            "celld_node_activating_cells",
            "Cold routes currently holding an activation permit.",
            metrics.activating,
        ),
        (
            "celld_node_activation_waiting_cells",
            "Cold routes queued behind the activation ceiling.",
            metrics.activation_waiting,
        ),
        (
            "celld_node_capacity_waiting_cells",
            "Cold routes queued behind the resident-cell ceiling.",
            metrics.capacity_waiting,
        ),
        (
            "celld_node_output_gate_pending_writes",
            "HTTP and WebSocket writes waiting for durable replication.",
            metrics.output_gate_pending,
        ),
    ] {
        help_and_type(&mut output, name, help, "gauge");
        let _ = writeln!(output, "{name} {value}");
    }

    help_and_type(
        &mut output,
        "celld_node_phase_cells",
        "Cells in each stable decision-core phase.",
        "gauge",
    );
    for phase in STABLE_PHASE_NAMES {
        let count = metrics
            .phases
            .iter()
            .find_map(|(candidate, count)| (*candidate == *phase).then_some(count))
            .copied()
            .unwrap_or_default();
        let _ = writeln!(
            output,
            "celld_node_phase_cells{{phase=\"{}\"}} {count}",
            label_value(phase),
        );
    }

    if let Some(limit) = metrics.resident_limit {
        help_and_type(
            &mut output,
            "celld_node_resident_limit_cells",
            "Configured hard resident-cell admission limit.",
            "gauge",
        );
        let _ = writeln!(output, "celld_node_resident_limit_cells {limit}");
        help_and_type(
            &mut output,
            "celld_node_resident_headroom_cells",
            "Remaining resident-cell admission headroom.",
            "gauge",
        );
        let _ = writeln!(
            output,
            "celld_node_resident_headroom_cells {}",
            limit.saturating_sub(metrics.occupied),
        );
    }

    help_and_type(
        &mut output,
        "celld_node_shedding",
        "Whether the node is currently shedding or refusing ownership.",
        "gauge",
    );
    let _ = writeln!(
        output,
        "celld_node_shedding {}",
        u8::from(metrics.shed_reason.is_some()),
    );
    help_and_type(
        &mut output,
        "celld_node_shedding_reason",
        "Current bounded decision-core shedding reason.",
        "gauge",
    );
    for reason in [SHED_MEMORY, SHED_RSS_HARD] {
        let _ = writeln!(
            output,
            "celld_node_shedding_reason{{reason=\"{}\"}} {}",
            label_value(reason),
            u8::from(metrics.shed_reason == Some(reason)),
        );
    }

    for (name, help, value) in [
        (
            "celld_process_resident_memory_bytes",
            "Resident set size sampled from the process allocator.",
            metrics.rss_bytes,
        ),
        (
            "celld_process_in_use_memory_bytes",
            "Bytes the allocator reports as currently in use.",
            metrics.in_use_bytes,
        ),
    ] {
        help_and_type(&mut output, name, help, "gauge");
        let _ = writeln!(output, "{name} {value}");
    }

    for (name, help, value) in [
        (
            "celld_node_publishes_total",
            "Runtime publications completed by this process.",
            metrics.publishes,
        ),
        (
            "celld_node_stops_total",
            "Runtime stops completed by this process.",
            metrics.stops,
        ),
        (
            "celld_node_ownership_acquired_total",
            "Ownership acquisitions accepted by the decision core.",
            metrics.activity.acquired,
        ),
        (
            "celld_node_proxied_requests_total",
            "Requests routed to a remote owner.",
            metrics.activity.proxied,
        ),
        (
            "celld_node_expired_owner_leases_total",
            "Expired owner leases observed by the decision core.",
            metrics.activity.expired_owner_leases,
        ),
        (
            "celld_node_restores_total",
            "Cell restores completed by the decision core.",
            metrics.activity.restored,
        ),
        (
            "celld_node_authority_epochs_advanced_total",
            "Ownership epochs advanced by the decision core.",
            metrics.activity.advanced_epochs,
        ),
    ] {
        help_and_type(&mut output, name, help, "counter");
        let _ = writeln!(output, "{name} {value}");
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_bounded_node_metrics_without_identifiers() {
        let output = render(&NodeMetrics {
            runtime_version: "0.2.1-coderabbit.test",
            region: "us-central1\"\\\n",
            ownership: "bucket",
            serving: true,
            occupied: 7,
            resident_limit: Some(10),
            evicting: 1,
            restoring: 2,
            activating: 1,
            activation_waiting: 1,
            capacity_waiting: 3,
            phases: &[("resident", 5), ("restoring", 2)],
            shed_reason: Some(SHED_MEMORY),
            rss_bytes: 1024,
            in_use_bytes: 768,
            output_gate_pending: 4,
            publishes: 8,
            stops: 2,
            activity: ActivitySnapshot {
                acquired: 11,
                proxied: 12,
                expired_owner_leases: 13,
                restored: 14,
                advanced_epochs: 15,
            },
        });

        assert!(output.contains("celld_build_info{version=\"0.2.1-coderabbit.test\",region=\"us-central1\\\"\\\\\\n\"} 1"));
        assert!(output.contains("celld_node_resident_headroom_cells 3"));
        assert!(output.contains("celld_node_phase_cells{phase=\"resident\"} 5"));
        assert!(output.contains("celld_node_phase_cells{phase=\"inactive\"} 0"));
        assert!(output.contains("celld_node_shedding_reason{reason=\"memory\"} 1"));
        assert!(output.contains("celld_node_shedding_reason{reason=\"rss-hard\"} 0"));
        assert!(output.contains("celld_node_output_gate_pending_writes 4"));
        assert!(output.contains("celld_node_authority_epochs_advanced_total 15"));
        assert!(!output.contains("cell_id"));
        assert!(!output.contains("request_id"));
        assert!(!output.contains("s3://"));
        assert!(!output.contains("bucket_name"));
    }

    #[test]
    fn omits_unbounded_limit_and_zeroes_bounded_series_when_inactive() {
        let output = render(&NodeMetrics {
            runtime_version: "test",
            region: "local",
            ownership: "memory",
            serving: true,
            occupied: 0,
            resident_limit: None,
            evicting: 0,
            restoring: 0,
            activating: 0,
            activation_waiting: 0,
            capacity_waiting: 0,
            phases: &[],
            shed_reason: None,
            rss_bytes: 0,
            in_use_bytes: 0,
            output_gate_pending: 0,
            publishes: 0,
            stops: 0,
            activity: ActivitySnapshot::default(),
        });

        assert!(!output.contains("celld_node_resident_limit_cells"));
        assert!(!output.contains("celld_node_resident_headroom_cells"));
        assert!(output.contains("celld_node_phase_cells{phase=\"resident\"} 0"));
        assert!(output.contains("celld_node_shedding_reason{reason=\"memory\"} 0"));
        assert!(output.contains("celld_node_shedding_reason{reason=\"rss-hard\"} 0"));
        assert!(output.contains("celld_node_shedding 0"));
    }
}
