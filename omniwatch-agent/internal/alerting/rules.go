// OmniWatch — Agent
// Component: alerting (rule file)
// Phase: industry-ready (IND-5)
// Purpose: Load PrometheusRule YAML (groups + rules) for otelcol rule processor
// Inputs: configs/alerts/prometheusrules.yaml path
// Outputs: Parsed RuleFile with flattened rules
package alerting

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// imageRulesPath is where the Dockerfile COPYs the rules in-image.
const imageRulesPath = "/etc/omniwatch/alerts/prometheusrules.yaml"

// RulesPath returns the rule file path: OMNIWATCH_ALERTING_RULES_PATH wins,
// otherwise the repo-relative default, otherwise the in-image path when the
// binary runs from / (distroless container). Local runs are unaffected.
func RulesPath() string {
	if v := os.Getenv("OMNIWATCH_ALERTING_RULES_PATH"); v != "" {
		return v
	}
	if _, err := os.Stat(DefaultRulesPath); err == nil {
		return DefaultRulesPath
	}
	if _, err := os.Stat(imageRulesPath); err == nil {
		return imageRulesPath
	}
	return DefaultRulesPath
}

// ApplyIntervalOverrides rewrites every group interval from
// OMNIWATCH_ALERTING_SCRAPE_INTERVAL (Go duration, e.g. "15s") and every rule
// For from OMNIWATCH_ALERTING_FOR_DEFAULT. Rule semantics (expr/labels) are
// untouched; empty env means no override.
func ApplyIntervalOverrides(f *RuleFile) {
	interval := os.Getenv("OMNIWATCH_ALERTING_SCRAPE_INTERVAL")
	forD := os.Getenv("OMNIWATCH_ALERTING_FOR_DEFAULT")
	if interval == "" && forD == "" {
		return
	}
	for gi := range f.Spec.Groups {
		if interval != "" {
			f.Spec.Groups[gi].Interval = interval
		}
		if forD != "" {
			for ri := range f.Spec.Groups[gi].Rules {
				f.Spec.Groups[gi].Rules[ri].For = forD
			}
		}
	}
}

// DefaultRulesPath is the repo-relative path of the production rule file,
// loaded by the otelcol rule processor (no Prometheus server is added).
const DefaultRulesPath = "configs/alerts/prometheusrules.yaml"

// Rule is a single PrometheusRule alerting rule: the alert name, the PromQL
// expression evaluated by the otelcol rule processor, the pending duration,
// and the labels/annotations attached on fire.
type Rule struct {
	Alert       string            `yaml:"alert"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

// RuleGroup is one named group of rules evaluated on a shared interval.
type RuleGroup struct {
	Name     string `yaml:"name"`
	Interval string `yaml:"interval"`
	Rules    []Rule `yaml:"rules"`
}

// RuleFile mirrors the PrometheusRule shape: metadata plus spec.groups.
// Only the fields the agent reads are modeled; unknown keys are ignored.
type RuleFile struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Spec struct {
		Groups []RuleGroup `yaml:"groups"`
	} `yaml:"spec"`
}

// LoadRules parses the PrometheusRule YAML at path. It rejects empty group
// lists and rules missing alert/expr so a malformed file fails fast at boot
// instead of silently exporting zero rules.
func LoadRules(path string) (*RuleFile, error) {
	if path == "" {
		return nil, fmt.Errorf("alerting: rule path is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("alerting: read rules %q: %w", path, err)
	}
	var rf RuleFile
	if err := yaml.Unmarshal(raw, &rf); err != nil {
		return nil, fmt.Errorf("alerting: parse rules %q: %w", path, err)
	}
	if len(rf.Spec.Groups) == 0 {
		return nil, fmt.Errorf("alerting: no rule groups in %q", path)
	}
	for _, g := range rf.Spec.Groups {
		for _, r := range g.Rules {
			if r.Alert == "" || r.Expr == "" {
				return nil, fmt.Errorf("alerting: rule in group %q missing alert/expr", g.Name)
			}
		}
	}
	return &rf, nil
}

// RuleCount returns the total number of rules across all groups.
func (f *RuleFile) RuleCount() int {
	n := 0
	for _, g := range f.Spec.Groups {
		n += len(g.Rules)
	}
	return n
}

// GroupedRule pairs a rule with its parent group name for export
// attributes. The embedded Rule promotes Alert/Expr/For/Labels so callers
// use gr.Alert directly.
type GroupedRule struct {
	Group string
	Rule
}

// Flatten returns every rule annotated with its group name.
func (f *RuleFile) Flatten() []GroupedRule {
	var out []GroupedRule
	for _, g := range f.Spec.Groups {
		for _, r := range g.Rules {
			out = append(out, GroupedRule{Group: g.Name, Rule: r})
		}
	}
	return out
}
