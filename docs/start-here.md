# Documentation Overview

This section provides configuration related information for using the Microsoft Purview Business Concepts autoloader accelerator.


## Related

- **[How to contribute](../CONTRIBUTING.md)**
- **[Our code of conduct](../CODE_OF_CONDUCT.md)**

## Folder Structure


- docs - documentation, process flows and state diagrams, and technical architecture.
- modules - consists of Microsoft Fabric notebooks with pySpark notebooks
- test - automated tests
- .vscode - configuration and recommendations for the VS Code IDE.
- .devcontainer - default configuration for GitHub codespaces or containerized development.

# Getting started
## Configuration

### Pre-requisites and set-up

Network enablement : Fabric Notebooks should be able to make Microsoft Catalog API calls to the Microsoft Purview Tenant service through an Entra Service Principal (SPN)

Access needed for SPN
1. Microsoft Purview Catalog Role : Business Domain Creator
2. Entra Graph API Directory.Read.All (Application), User.Read (Delegated), User.Read.All (Application)
3. Azure Blob Storage : Storage Account Blob Reader
4. Azure Key Vault : Key Vault Reader access
![image](https://github.com/user-attachments/assets/0256a411-4497-4184-a978-0f5d597bdee4)

