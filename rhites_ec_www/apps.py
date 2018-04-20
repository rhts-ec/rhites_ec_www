from easyaudit.apps import EasyAuditConfig

# Change "Easy Audit Application" to "Audit Log"
class AuditLogConfig(EasyAuditConfig):
    verbose_name = 'Audit Log'
