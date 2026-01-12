# Unity Catalog access request — {{request_title}}

**Requester:** {{requester_name}} ({{requester_email}})  
**Team:** {{team}}  
**Date:** {{date}}  
**Urgency:** {{urgency}} (Low/Medium/High)  

## 1) What do you need access to?
- **Object type:** {{object_type}} (catalog / schema / table / view / function / volume)
- **Full name:** {{full_name}} (example: `main.analytics.customer_churn`)
- **Environment:** {{environment}} (dev/stage/prod)

## 2) What level of access?
- Requested privileges: {{privileges}} (e.g., SELECT, MODIFY, EXECUTE, USAGE)
- Duration: {{duration}} (temporary until {{end_date}} / permanent)

## 3) Justification
Explain the business need, ticket link, and why least-privilege is sufficient.

- Ticket / link: {{ticket_link}}
- Use case: {{use_case}}
- Data classification expected: {{classification}} (PII/PHI/PCI/None/Unknown)

## 4) Approvals
- Data owner approval: {{data_owner}} (✅/pending)
- Security / compliance approval (if required): {{security_approval}}

## 5) Optional: GRANT SQL (to be executed by an admin)
> Note: adjust privileges for least privilege. Use Unity Catalog groups (not individual users) when possible.

```sql
-- Example: grant schema usage + table select
GRANT USAGE ON CATALOG {{catalog}} TO `{{group}}`;
GRANT USAGE ON SCHEMA {{catalog}}.{{schema}} TO `{{group}}`;
GRANT SELECT ON TABLE {{catalog}}.{{schema}}.{{table}} TO `{{group}}`;
```

## 6) Completion checklist
- [ ] Request reviewed for least privilege
- [ ] Owner approvals captured
- [ ] Grants applied
- [ ] Access verified by requester
- [ ] Request closed / documented
