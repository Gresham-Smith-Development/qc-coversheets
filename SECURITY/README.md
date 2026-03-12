# QC Coversheet Application

## _Security Architecture Overview_


## 1\. Executive Summary

The **QC Coversheet (QCC) Application** is an internally managed workflow application designed to facilitate **external subconsultant quality review submissions** associated with project deliverables within **Deltek Vantagepoint**.

The system provides a secure workflow allowing project managers to request quality review confirmation from external subconsultants. External users are temporarily granted controlled access to submit responses, after which their access is automatically revoked.

The application is hosted on a **secured Azure Linux Virtual Machine** and is designed with several key security controls:

-   **Microsoft Entra ID authentication (OIDC)** for all authenticated user access
    
-   **Automated B2B guest access management** for external subconsultants
    
-   **HMAC-signed REST requests** from Vantagepoint for workflow initiation
    
-   **Microsoft Graph integration** for identity management and email automation
    
-   **Security group and application role controls** for authorization
    
-   **Network isolation and firewall protections**
    
-   **Secrets stored securely in Secret Server**
    

The system does **not store project documents or attachments** and only captures structured review responses consisting of radio button selections and limited free-text notes.

This document outlines the **security architecture, access model, automation workflows, and external dependencies** required for cybersecurity review and approval.

For an in-depth view of the application order of operations, click [HERE](order_of_app_operations.md).


## 2\. My Ask List (Cybersecurity Review)

Approval is requested for the following architectural components:

### Identity and Application Registration

-   Permission to create/extend **Microsoft Entra App Registration for QC Coversheet Application**
    
-   Approval of **Application Roles**
    
-   Approval of **Microsoft Graph permissions**
    

### External Access

-   Approval for **Microsoft Entra B2B guest access model**
    
-   Approval of **automated invitation workflow**
    

### Microsoft Graph Permissions

Approval for the following application permissions:

-   Mail.Send
    
-   GroupMember.ReadWrite.All
    
-   User.Invite.All
    
-   User.ReadWrite.All
    
-   Directory.Read.All
    

### Service Email Account

Approval to create a **dedicated service mailbox** used for automated notifications.

### Security Groups

Creation of required Entra security groups:

-   QC-Coversheet-Ext-Editor
    
-   Internal reviewer groups (existing organizational groups leveraged)
    

### Infrastructure Architecture

Approval of the hosting architecture:

-   Azure Linux VM
    
-   Reverse proxy (NGINX)
    
-   PostgreSQL database hosted on the same secured VM
    


## 3\. Expanded Finalization Requirements

### Microsoft Entra Application Registration

A dedicated Entra App Registration is required to:

-   support application authentication
    
-   manage Microsoft Graph access
    
-   define application roles used by the system
    

Roles defined within the application:

| Role | Purpose |
| --- | --- |
| Admin | Application administration and maintenance |
| Reviewer | External subconsultant reviewer access |
| InternalReadOnly | Internal staff read-only review visibility |
| User | Default role with no permissions |


### Microsoft Graph Permissions

The application uses Microsoft Graph for identity and automation functions including:

-   Creating B2B guest invitations
    
-   Adding and removing users from security groups
    
-   Sending automated notification emails
    
-   Managing external reviewer lifecycle
    

Two app registrations are planned:

**1\. Access Management Application**  
Used for identity automation and user management.

Permissions required:

-   GroupMember.ReadWrite.All
    
-   User.Invite.All
    
-   User.ReadWrite.All
    
-   Directory.Read.All
    

**2\. Email Automation Application**

Used only for sending automated system notifications.

Permission required:

-   Mail.Send (Application)
    


### Service Mailbox for Email Automation

A dedicated service mailbox will be used for automated system notifications.

Example naming convention:

```
qcc-notifications@greshamsmith.com
```

Security control:

-   Microsoft Graph Mail.Send permission will be restricted using **Exchange Application RBAC**
    
-   The application will only be permitted to send email through the designated service mailbox
    

This prevents misuse of broader tenant mailbox permissions.


### External Subconsultant Access Model

External reviewers are invited as **Microsoft Entra B2B Guest users**.

Access is managed through the security group:

```
QC-Coversheet-Ext-Editor
```

Workflow:

1.  Vantagepoint workflow triggers application ingest process
    
2.  Application retrieves reviewer information
    
3.  Microsoft Graph sends B2B invitation
    
4.  Guest user is added to reviewer security group
    
5.  Reviewer completes QC Coversheet form
    
6.  Access is automatically removed after submission or expiration
    

Access duration:

-   Submission deadline
    
-   Plus configurable grace period (~15 days)
    


### REST Ingest Security (Vantagepoint → Application)

Workflow initiation occurs via a REST request from Vantagepoint.

Security controls:

-   HMAC request signature validation
    
-   Custom headers:
    

```
X-Timestamp  
X-Signature
```

The request signature is validated using a shared secret stored in **Secret Server**.

The ingest endpoint is the **only publicly accessible unauthenticated endpoint**.



### Authentication Model

All authenticated user interactions use:

**Microsoft Entra ID OpenID Connect (OIDC)**

Authentication flow:

1.  User redirected to Entra ID login
    
2.  ID token returned to application
    
3.  Application validates token
    
4.  Application maps Entra roles and group membership
    
5.  Session established for application access
    

Authorization decisions are enforced using:

-   App roles
    
-   Security group membership
    
-   Application-level permission checks
    


### Infrastructure and Network Security

The application is hosted on an **Azure Linux Virtual Machine**.

Architecture components:

-   FastAPI application service
    
-   PostgreSQL database
    
-   NGINX reverse proxy
    
-   Background automation jobs
    

Network protection:

-   Azure Network Security Group (NSG)
    
-   Azure Firewall
    

Only **HTTPS (80 & 443)** are exposed publicly (current GIA VM settings).


### Reverse Proxy Security Layer

NGINX is used as a reverse proxy and provides:

-   TLS/HTTPS termination
    
-   Reverse proxy routing to the FastAPI application
    
-   Basic request filtering
    
-   Rate limiting capabilities
    


### Database Security

The application uses **PostgreSQL** hosted on the same secured Azure VM.

Security controls:

-   Database runs inside Docker
    
-   Accessible only by the application service
    
-   Not publicly exposed
    


### Secret Management

Sensitive credentials are stored in **Secret Server**.

Secrets include:

-   Microsoft Graph credentials
    
-   Vantagepoint credentials
    
-   HMAC signing secret
    
-   Application session secret
    
-   Database connection credentials
    

Development environments use environment variables, while production plans to retrieve secrets dynamically from Secret Server.


### Logging and Monitoring

Logging includes:

-   access control changes
    
-   ingest workflow events
    
-   system processing activity
    

Authentication logging is handled by **Microsoft Entra ID audit logs**.

Application logs are currently written to **local log files** on the server.


### Automation Workflows

Several automated background processes support the system workflow.

These processes run on the **same secured VM** using scheduled cron jobs.

Responsibilities include:

-   sending reminder notifications
    
-   monitoring review submission deadlines
    
-   revoking reviewer access after completion
    
-   updating workflow status
    


### Data Classification

The application stores limited metadata only, including:

-   reviewer names
    
-   reviewer email addresses
    
-   project identifiers
    
-   review completion status
    

No documents, attachments, or proprietary deliverables are stored.

User input is limited to:

-   radio button selections
    
-   short text notes
    


### Future Enhancement: Email Platform Integration

A potential future architecture enhancement would integrate with the enterprise Mailchimp platform for outbound communications.

Under this model:

-   the application would provide recipient and message content
    
-   Mailchimp would manage delivery, compliance, and campaign management
    

This would reduce reliance on Microsoft Graph email permissions while improving communication governance.

