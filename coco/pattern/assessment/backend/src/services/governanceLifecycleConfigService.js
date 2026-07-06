'use strict';

// Governance Lifecycle — runtime-editable content storage.
//
// Unlike questions.json (frontend/backend build-time seed data), this content
// is expected to change on a planning-meeting cadence, not a release cadence
// — teams, phases, RACI assignments, and SOP text all get revised as the
// governance model solidifies. So it lives in backend/data/ (gitignored,
// mounted as a volume/PVC in Docker/Helm), editable via GET/PUT routes and
// the frontend's "Edit Content" panel, exactly like Payload Library's YAML
// editor and OPA/Ranger's policy editors — no rebuild/redeploy needed to
// change it.
//
// No encryption here (unlike payloadService.js/opaPolicyPersistService.js) —
// this is org-chart and process documentation, not credentials or policy
// secrets, so AES-256-GCM would be ceremony without benefit.

const fs   = require('fs');
const path = require('path');

function storagePath() {
  return (
    process.env.GOVERNANCE_LIFECYCLE_STORAGE_PATH ||
    path.join(__dirname, '../../data/governance-lifecycle')
  );
}

function configFilePath() {
  return path.join(storagePath(), 'config.json');
}

// Seed content — served whenever no config.json has been saved yet. This is
// the same content that previously lived in
// frontend/src/data/governanceLifecycle.config.js, with the iam_src -> iam_sre
// typo fixed and the duplicate "dgo" key in the first RACI row deduped.
const DEFAULT_CONFIG = {
  app: {
    kicker: 'Secure ABAC Policy Governance Lifecycle',
    title: 'PlainID Policy Deployment Lifecycle',
    swimlaneIntro:
      'Each phase below moves an access request from requirement intake to production deployment. Click any phase to inspect full operational detail in the drawer below.',
    rejectionToggleLabel: '🔄 Simulate CAB Rejection & Loop',
    footer: 'ABAC Policy Governance Lifecycle Presentation • Internal Walking-Deck Tool',
  },

  teams: [
    { id: 'app', shortLabel: 'APP', label: 'Application/Business Team', colorKey: 'blue' },
    { id: 'dgo', shortLabel: 'DGO', label: 'Data Governance Office', colorKey: 'teal' },
    { id: 'adm', shortLabel: 'ADM', label: 'Access Data Management', colorKey: 'violet' },
    { id: 'iam_sre', shortLabel: 'IAM/SRE', label: 'IAM/SRE Platform Team', colorKey: 'emerald' },
    { id: 'udap', shortLabel: 'UDAP', label: 'Unified Data Analytics Platform Team', colorKey: 'indigo' },
    { id: 'dpe', shortLabel: 'DPE', label: 'Data Platform Engineering Team', colorKey: 'cyan' },
    { id: 'ad', shortLabel: 'AD', label: 'Active Directory Team', colorKey: 'amber' },
    { id: 'cab', shortLabel: 'CAB', label: 'Change Advisory Board', colorKey: 'rose' },
  ],

  phases: [
    {
      id: 1,
      code: '01',
      teamIds: ['app', 'dgo', 'ad', 'dpe', 'adm'],
      colorKey: 'blue',
      title: 'Requirements, Role Mapping & AD Provisioning',
      subtitle: 'Business Intake, Compliance Confirmation, Catalog Role Mapping & AD Setup',
      teaser:
        'Business requirements are documented, DGO confirms sensitive fields against compliance policy, and requests are mapped to catalog roles and AD groups.',
      scope:
        'The Application/Business team documents access requirements and shares them with the Data Governance Office (DGO), which defines the applicable compliance policy and confirms which fields the Application team has identified are actually sensitive. UDAP then maps the confirmed requirement to data catalog role definitions. Any new access role is requested from the AD team, which creates the corresponding AD group and adds the requesting users. In parallel, the Data Platform Engineering team (DPE) applies and manages the ABAC tags on the relevant data catalog objects, coordinating with UDAP, so PlainID policies have a consistent tagging surface to reference.',
      activities: [
        'Document and share the access requirement with the governing teams.',
        'Define or confirm the applicable compliance policy for the requirement.',
        'Confirm which fields identified by the Application team are sensitive.',
        'Map the confirmed requirement to data catalog role definitions.',
        'Request the corresponding role in Active Directory.',
        'Create the requested role as an AD group.',
        'Add the requesting users to the AD group.',
        'Apply and manage the ABAC tags on the relevant catalog objects.',
      ],
      inputs: [
        'Business access request',
        'Data catalog schema',
        'Existing AD group inventory',
        'Compliance policy framework',
      ],
      outputs: [
        'Approved requirement document',
        'Confirmed sensitive fields list',
        'Data catalog role mapping',
        'Provisioned AD group with membership',
        'ABAC tags applied to catalog objects',
      ],
      owner: 'Application/Business Owner, DGO Compliance Lead & UDAP Data Steward',
      sla: '3 to 5 business days for compliance confirmation, role mapping, and AD provisioning',
      systems: ['Requirement tracking system', 'Databricks Unity Catalog', 'Azure Active Directory'],
      receivesRemediationUpdates: false,
    },
    {
      id: 2,
      code: '02',
      teamIds: ['app', 'adm'],
      colorKey: 'violet',
      title: 'Test Data Prep & PlainID Policy Authoring',
      subtitle: 'Smoke Test Assets & PEP ABAC Policy Drafting',
      teaser:
        'The Application team stages smoke test use cases and data while ADM authors the PlainID policy and PEP ABAC configuration.',
      scope:
        'The Application team creates representative use cases and sample data for smoke testing the new access policy. ADM authors the policy directly in PlainID, configures the POP settings, and drafts the PEP ABAC policy definition, submitting it as a Git pull request for review.',
      activities: [
        'Create use cases for smoke testing the new policy.',
        'Create sample data for smoke testing.',
        'Create the policy in PlainID.',
        'Configure POP settings in PlainID.',
        'Author the PEP ABAC policy and submit it as a Git pull request.',
      ],
      inputs: ['Approved requirement document', 'Data catalog role mapping', 'AD group membership'],
      outputs: ['Smoke test use cases and sample data', 'PlainID policy definition', 'PEP ABAC Git pull request'],
      owner: 'ADM Policy Author',
      sla: '2 to 4 business days for authoring and staging',
      systems: ['PlainID console', 'Git repository', 'Databricks test workspace'],
      receivesRemediationUpdates: true,
    },
    {
      id: 3,
      code: '03',
      teamIds: ['adm', 'dpe', 'udap'],
      colorKey: 'indigo',
      title: 'Policy Review, Approval & Propagation',
      subtitle: 'DPE PR Approval, Databricks Application & PlainID Propagation',
      teaser:
        'DPE approves the PEP ABAC pull request and applies the policy in Databricks while IAM/SRE propagates it through PlainID and UDAP confirms behavior.',
      scope:
        'The Data Platform Engineering team (DPE) reviews and approves the PEP ABAC Git pull request submitted by ADM, then applies the approved policy directly to the target Databricks catalog objects. IAM/SRE propagates the policy through the PlainID platform, and UDAP runs a sanity check to confirm the policy behaves as expected.',
      activities: [
        'Review and approve the PEP ABAC policy pull request.',
        'Apply the approved PEP ABAC policy to the target Databricks catalog objects.',
        'Propagate the approved policy through the PlainID platform.',
        'Run a PEP ABAC sanity check to confirm expected behavior.',
      ],
      inputs: ['PEP ABAC Git pull request', 'PlainID policy definition'],
      outputs: [
        'Approved and merged policy pull request',
        'Policy applied to catalog objects',
        'Propagated PlainID policy',
        'Sanity check results',
      ],
      owner: 'DPE & IAM/SRE Platform Owners',
      sla: '2 to 3 business days for review, application, and propagation',
      systems: ['Git repository', 'Databricks Unity Catalog', 'PlainID platform'],
      receivesRemediationUpdates: true,
    },
    {
      id: 4,
      code: '04',
      teamIds: ['app', 'adm', 'dpe', 'udap'],
      colorKey: 'amber',
      title: 'Protection Validation & Business Sign-off',
      subtitle: 'Use Case Verification & Formal Protection Sign-off',
      teaser:
        'The Application team verifies the protected data behaves correctly against their use cases and formally signs off before promotion.',
      scope:
        'The Application team verifies the user data and use case results against the newly applied ABAC protection, with UDAP supporting technical validation. Once confirmed, the Application team formally signs off that the protection meets business requirements and is ready for production promotion.',
      activities: [
        'Verify user data and use case results against the applied protection.',
        'Validate that the protection behaves as intended for every use case.',
        'Formally sign off that protection is ready for production promotion.',
      ],
      inputs: ['Sanity check results', 'Smoke test use cases and sample data'],
      outputs: ['Validated protection results', 'Business sign-off record'],
      owner: 'Application/Business Owner',
      sla: '1 to 2 business days for validation and sign-off',
      systems: ['Databricks test workspace', 'Sign-off tracking system'],
      receivesRemediationUpdates: true,
    },
    {
      id: 5,
      code: '05',
      teamIds: ['cab', 'iam_sre'],
      colorKey: 'emerald',
      title: 'PROD Promotion & Support',
      subtitle: 'Change Board Approval, Deployment & Hypercare',
      teaser:
        'CAB approves the production promotion, IAM/SRE deploys the policy, and hypercare support covers the post-deployment window.',
      scope:
        'The Change Advisory Board reviews and approves the promotion of the signed-off policy into the production environment. IAM/SRE executes the production deployment, then provides hypercare support through the initial stabilization window before transitioning to standard production support.',
      activities: [
        'Submit the promotion request to the Change Advisory Board for approval.',
        'Deploy the approved policy to the production environment.',
        'Provide hypercare support through the post-deployment stabilization window.',
        'Transition to standard production support.',
      ],
      inputs: ['Business sign-off record', 'Validated protection results'],
      outputs: ['CAB-approved change record', 'Production-deployed ABAC policy', 'Hypercare support log'],
      owner: 'CAB & IAM/SRE Deployment Lead',
      sla: '1 business day for CAB review, same-day deployment upon approval',
      systems: ['Change Management Board tool', 'PlainID production environment', 'Production support ticketing system'],
      receivesRemediationUpdates: false,
    },
  ],

  rejectionLoop: {
    statusActiveLabel: 'Rejection Loop Active',
    statusNominalLabel: 'Lifecycle Nominal',
    cardFlagLabel: '🔄 Receiving Updates...',
    drawerFlagLabel: '🔄 Receiving Remediation Updates',
    swimlaneBannerTitle: 'CAB Rejection Loop Engaged — Triage Workflow Active',
    swimlaneBannerBody:
      'CAB rejected the production promotion request during change review. Execution control has been routed backward: ADM is revisiting the PlainID policy and PEP ABAC configuration, DPE is re-validating the pull request approval and Databricks catalog object application, IAM/SRE is re-validating policy propagation, and UDAP is re-confirming the sanity checks. The Application team re-verifies protection sign-off before the corrected promotion request is resubmitted to CAB.',
    directiveActive: {
      title: 'Operational Rejection Remediation Directive',
      badge: '🔄 Active',
      body:
        'The CAB rejection loop is currently engaged. ADM, DPE, IAM/SRE, and UDAP are actively co-authoring corrective updates to the PEP ABAC policy in response to a CAB rejection. Do not resubmit the promotion request until protection has been re-validated and signed off again by the Application team.',
      bullets: [
        'Freeze new promotion requests for the affected policy until remediation is complete.',
        'ADM must reuse the existing PEP ABAC pull request rather than opening a new one.',
        'CAB re-review SLA resets to 1 business day for remediation cycles.',
      ],
    },
    directiveStandby: {
      title: 'Operational Rejection Remediation Directive',
      badge: 'Standby',
      body:
        'Standing by. This directive activates automatically whenever CAB rejects a production promotion request, routing remediation ownership back to ADM, DPE, IAM/SRE, and UDAP. Toggle "Simulate CAB Rejection & Loop" above to preview the active remediation directive.',
    },
  },

  raci: {
    intro:
      'Each governance activity maps to exactly one accountable owner. Responsible, consulted, and informed parties round out execution and communication paths across every governance team.',
    rows: [
      { activity: 'Requirement Sharing/Document creation', assignments: { app: 'A', dgo: 'C', udap: 'C', adm: 'I' } },
      { activity: 'Compliance Policy Definition', assignments: { dgo: 'A' } },
      { activity: 'Sensitive Fields Confirmation', assignments: { dgo: 'A', app: 'C' } },
      { activity: 'Data catalog - defined role mapping', assignments: { app: 'RA', adm: 'C' } },
      { activity: 'Request of Role in AD', assignments: { adm: 'RA', dgo: 'C', ad: 'I' } },
      { activity: 'Creation of Role in AD', assignments: { ad: 'R', adm: 'A', app: 'CI', dpe: 'CI' } },
      { activity: 'Addition of user to the Role in AD', assignments: { ad: 'R', app: 'A' } },
      { activity: 'Create/manage ABAC tags', assignments: { dpe: 'CI', adm: 'RA', dgo: 'C' } },
      { activity: 'Use case creation for smoke testing', assignments: { adm: 'RA', app: 'C' } },
      { activity: 'Data creation for smoke testing', assignments: { adm: 'RA', app: 'C' } },
      { activity: 'User data and Use case verification / sign-off', assignments: { app: 'A', adm: 'R' } },
      { activity: 'Policy Creation in PlainID', assignments: { adm: 'A', iam_sre: 'C' } },
      { activity: 'POP Configuration in PlainID', assignments: { adm: 'A', iam_sre: 'C' } },
      { activity: 'PEP ABAC Policy creation + Git PR', assignments: { adm: 'RA', udap: 'I' } },
      { activity: 'PEP ABAC Policy PR Approvals', assignments: { adm: 'R', dpe: 'A' } },
      { activity: 'Policy propagation', assignments: { app: 'I', adm: 'A', dpe: 'R' } },
      { activity: 'PEP ABAC Policy (Apply Catalog Objects)', assignments: { app: 'I', adm: 'A', dpe: 'R' } },
      { activity: 'PEP ABAC Sanity Check', assignments: { adm: 'RA' } },
      { activity: 'Protection - Validation', assignments: { app: 'RA', adm: 'C', udap: 'I' } },
      { activity: 'Protection - Sign-off', assignments: { app: 'RA', adm: 'C', udap: 'I', iam_sre: 'I' } },
      { activity: 'PROD promotion - CAB', assignments: { cab: 'R', adm: 'RA', dpe: 'I', iam_sre: 'I' } },
      { activity: 'PROD promotion - Deploy', assignments: { adm: 'RA', dpe: 'I', iam_sre: 'I' } },
      { activity: 'Hypercare Support', assignments: { iam_sre: 'A', adm: 'R', app: 'C' } },
      { activity: 'PROD Support', assignments: { iam_sre: 'A', app: 'C', adm: 'C', udap: 'C', dpe: 'I' } },
    ],
    definitions: [
      { letter: 'R', label: 'Responsible', desc: 'Executes the work required to complete the activity.' },
      { letter: 'A', label: 'Accountable', desc: 'Owns the outcome and holds final sign-off authority. Exactly one per row.' },
      { letter: 'C', label: 'Consulted', desc: 'Provides input and subject-matter expertise before execution.' },
      { letter: 'I', label: 'Informed', desc: 'Kept up to date on progress and outcomes after the fact.' },
    ],
  },

  sop: {
    sections: [
      {
        id: 'overview',
        label: 'Overview & Lifecycle Trigger',
        trigger: 'A new or modified data access requirement enters the ABAC governance intake queue.',
        intro:
          'This runbook governs the end-to-end secure ABAC policy governance lifecycle for provisioning PlainID-based access control across Databricks catalogs. Teams operate in strict sequence — the Application/Business team, DGO, UDAP, DPE, ADM, the AD team, IAM/SRE, and CAB — with a defined exception path back to the originating phases whenever CAB rejects a production promotion request.',
        steps: [
          'Confirm the access requirement has been logged and shared as a formal document.',
          'Assign an Application/Business owner, a DGO compliance reviewer, and a UDAP data steward to open Phase 1.',
          'Track the lifecycle through each sequential phase using the Swimlane Presentation view.',
          'Escalate to the Exception Handling runbook immediately if CAB rejects the promotion request at Phase 5.',
        ],
        exit: [
          'Requirement document, AD provisioning record, PlainID policy, sign-off record, and production deployment are each recorded with a timestamp and responsible owner.',
        ],
      },
      {
        id: 'phase1',
        label: 'Phase 1 — Requirements, Role Mapping & AD Provisioning',
        trigger: 'Application/Business team submits a new access requirement for governance review.',
        intro:
          'The Application/Business team documents the requirement, DGO defines the applicable compliance policy and confirms which fields are sensitive, UDAP maps the confirmed requirement to data catalog roles, the AD team provisions the corresponding AD group, and DPE applies and manages the ABAC tags on the relevant catalog objects before any policy is authored.',
        steps: [
          'Document and share the access requirement with the governing teams.',
          'Define or confirm the applicable compliance policy for the requirement.',
          'Confirm which fields identified by the Application team are sensitive.',
          'Map the confirmed requirement to data catalog role definitions.',
          'Request the corresponding role in Active Directory.',
          'Create the requested role as an AD group.',
          'Add the requesting users to the AD group.',
          'Apply and manage the ABAC tags on the relevant catalog objects.',
        ],
        exit: [
          'DGO has confirmed the sensitive fields against compliance policy.',
          'AD group is created and populated with the correct users.',
          'ABAC tags are applied to the relevant catalog objects.',
        ],
      },
      {
        id: 'phase2',
        label: 'Phase 2 — Test Data Prep & PlainID Policy Authoring',
        trigger: 'AD group and ABAC tags from Phase 1 are ready for policy authoring.',
        intro:
          'The Application team stages smoke test use cases and data while ADM authors the PlainID policy, configures POP settings, and drafts the PEP ABAC policy for review.',
        steps: [
          'Create use cases for smoke testing the new policy.',
          'Create sample data for smoke testing.',
          'Create the policy in PlainID.',
          'Configure POP settings in PlainID.',
          'Author the PEP ABAC policy and submit it as a Git pull request to Phase 3.',
        ],
        exit: [
          'Smoke test use cases and sample data are staged and ready.',
          'PEP ABAC pull request is open and assigned to DPE for review.',
        ],
      },
      {
        id: 'phase3',
        label: 'Phase 3 — Policy Review, Approval & Propagation',
        trigger: 'PEP ABAC pull request is submitted by ADM and assigned for DPE review.',
        intro:
          'DPE reviews and approves the PEP ABAC pull request and applies the policy to the target Databricks catalog objects, IAM/SRE propagates it through PlainID, and UDAP runs a sanity check to confirm expected behavior.',
        steps: [
          'Review and approve the PEP ABAC policy pull request.',
          'Apply the approved PEP ABAC policy to the target Databricks catalog objects.',
          'Propagate the approved policy through the PlainID platform.',
          'Run a PEP ABAC sanity check to confirm expected behavior.',
        ],
        exit: [
          'Policy pull request is approved, merged, and applied to catalog objects.',
          'Sanity check confirms the policy behaves as expected after propagation.',
        ],
      },
      {
        id: 'phase4',
        label: 'Phase 4 — Protection Validation & Business Sign-off',
        trigger: 'Sanity check results from Phase 3 are ready for business validation.',
        intro:
          'The Application team verifies user data and use case results against the applied protection, with UDAP supporting technical validation, then formally signs off for production promotion.',
        steps: [
          'Verify user data and use case results against the applied protection.',
          'Validate that the protection behaves as intended for every use case.',
          'Formally sign off that protection is ready for production promotion.',
        ],
        exit: [
          'Protection validation is complete with no outstanding issues.',
          'Business sign-off record is filed ahead of Phase 5.',
        ],
      },
      {
        id: 'phase5',
        label: 'Phase 5 — PROD Promotion & Support',
        trigger: 'Business sign-off record from Phase 4 is submitted for change approval.',
        intro:
          'The Change Advisory Board reviews and approves the promotion, IAM/SRE deploys the policy to production, and hypercare support covers the post-deployment window.',
        steps: [
          'Submit the promotion request to the Change Advisory Board for approval.',
          'Deploy the approved policy to the production environment.',
          'Provide hypercare support through the post-deployment stabilization window.',
          'Transition to standard production support.',
        ],
        exit: [
          'CAB approves the change record with no outstanding findings.',
          'Policy is deployed to production and hypercare support has concluded.',
        ],
      },
      {
        id: 'exception',
        label: 'Exception Handling — CAB Rejection & Remediation Loop',
        trigger: 'CAB rejects the production promotion request during change review.',
        intro:
          'This directive governs the corrective loop that activates when a production promotion request fails CAB review. Control is routed backward to the phases responsible for the underlying policy definition and validation rather than restarting the full lifecycle from intake.',
        steps: [
          'CAB itemizes the rejection findings on the change record and notifies ADM, DPE, IAM/SRE, and UDAP.',
          'ADM revisits the PlainID policy and PEP ABAC configuration implicated by the findings.',
          'DPE re-reviews and re-approves the corrected pull request and re-applies the policy to catalog objects.',
          'IAM/SRE re-validates the propagation of the corrected policy through PlainID.',
          'UDAP reruns the sanity check to confirm the corrected policy behaves as expected.',
          'The Application team re-verifies protection and signs off again before resubmission to CAB.',
        ],
        exit: [
          'Promotion request passes CAB review with no outstanding findings.',
          'IAM/SRE completes production deployment and hypercare support begins.',
        ],
      },
    ],
  },
};

/**
 * Validates the shape and cross-references of a governance lifecycle config.
 * Returns an array of human-readable error strings — empty array means valid.
 *
 * Enforced invariants:
 *   - teams[].id values are unique
 *   - every phases[].teamIds entry resolves to a known team id
 *   - every raci.rows[].assignments key resolves to a known team id
 *   - every raci.rows[] has exactly one team whose assignment includes 'A'
 */
function validateConfig(config) {
  const errors = [];

  if (!config || typeof config !== 'object') {
    return ['Config must be a JSON object'];
  }
  if (!Array.isArray(config.teams) || config.teams.length === 0) {
    errors.push('"teams" must be a non-empty array');
    return errors; // can't validate cross-references without a team list
  }
  if (!Array.isArray(config.phases)) {
    errors.push('"phases" must be an array');
  }
  if (!config.raci || !Array.isArray(config.raci.rows)) {
    errors.push('"raci.rows" must be an array');
  }

  const teamIds = config.teams.map((t) => t.id);
  const teamIdSet = new Set(teamIds);
  if (teamIdSet.size !== teamIds.length) {
    errors.push('Duplicate team id found in "teams" — every team id must be unique');
  }

  (config.phases || []).forEach((phase, i) => {
    (phase.teamIds || []).forEach((id) => {
      if (!teamIdSet.has(id)) {
        errors.push(`phases[${i}] ("${phase.title || phase.id}") references unknown team id "${id}"`);
      }
    });
  });

  (config.raci?.rows || []).forEach((row, i) => {
    const assignmentKeys = Object.keys(row.assignments || {});
    assignmentKeys.forEach((id) => {
      if (!teamIdSet.has(id)) {
        errors.push(`raci.rows[${i}] ("${row.activity}") references unknown team id "${id}"`);
      }
    });
    const accountableCount = Object.values(row.assignments || {}).filter((v) => v.includes('A')).length;
    if (accountableCount !== 1) {
      errors.push(
        `raci.rows[${i}] ("${row.activity}") has ${accountableCount} Accountable owner(s) — expected exactly 1`
      );
    }
  });

  return errors;
}

/**
 * Reads the saved config from storage, or the built-in default if no
 * config.json has ever been saved. Returns { config, usingDefault }.
 */
function readConfig() {
  const filePath = configFilePath();
  if (fs.existsSync(filePath)) {
    try {
      const config = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      return { config, usingDefault: false };
    } catch (err) {
      console.error(`[governanceLifecycleConfigService] Failed to parse ${filePath}, serving default:`, err.message);
      return { config: DEFAULT_CONFIG, usingDefault: true };
    }
  }
  return { config: DEFAULT_CONFIG, usingDefault: true };
}

/**
 * Validates and persists a new config. Throws with a `validationErrors`
 * array property if validation fails — the route layer maps that to a 400.
 */
function writeConfig(config) {
  const errors = validateConfig(config);
  if (errors.length > 0) {
    const err = new Error('Governance Lifecycle config failed validation');
    err.validationErrors = errors;
    throw err;
  }
  const dir = storagePath();
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(configFilePath(), JSON.stringify(config, null, 2), 'utf8');
}

module.exports = { readConfig, writeConfig, validateConfig, DEFAULT_CONFIG };
