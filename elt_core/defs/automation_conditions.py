from typing import Optional

from dagster import AutomationCondition
from typing import override

from dagster._core.definitions.declarative_automation.operators import AndAutomationCondition

_AUTOMATION_IGNORE_SELECTION = None



class CustomAutomationCondition(AutomationCondition):
    
    @classmethod
    def get_automation_condition(cls, automation_condition_name: str) -> Optional[AutomationCondition]:
        methods = AutomationCondition.__dict__ | cls.__dict__
        return methods.get(automation_condition_name, None)      

    @staticmethod
    def manual():
         return None

    @staticmethod
    def on_cron_no_deps(cron_schedule: str, cron_timezone: str = "utc") -> AutomationCondition:
        """Returns an AutomationCondition which will cause a target to be executed on a given
        cron schedule, regardless of the state of its dependencies

        For time partitioned assets, only the latest time partition will be considered.
        """
        return (
            AutomationCondition.in_latest_time_window()
            & AutomationCondition.cron_tick_passed(
                cron_schedule, cron_timezone
            ).since_last_handled()
        ).with_label(f"on_cron_no_deps({cron_schedule}, {cron_timezone})")


    @override
    @staticmethod
    def eager() -> AndAutomationCondition:
            return (
                AutomationCondition.in_latest_time_window()
                & (
                    AutomationCondition.newly_missing() | AutomationCondition.any_deps_updated()
                ).since_last_handled()
                & ~AutomationCondition.any_deps_missing()
                & ~AutomationCondition.any_deps_in_progress()
                & ~AutomationCondition.in_progress()
            ).with_label("eager")


    @staticmethod
    def missing_or_changed() -> AutomationCondition:
        return (
            AutomationCondition.in_latest_time_window()
            & (
                AutomationCondition.code_version_changed() | AutomationCondition.newly_missing()
            ).since_last_handled()
            & ~ AutomationCondition.in_progress()
        ).with_label(f"missing_or_changed")
    

    @classmethod
    def lazy(cls) -> AutomationCondition:
        """True if any downstream conditions are true or the partition is missing or changed"""
        return(
            AutomationCondition.any_downstream_conditions() | cls.missing_or_changed()
        ).with_label("lazy")
    
    @staticmethod
    def lazy_on_cron(
         cron_schedule: str, cron_timezone: str = "UTC"
    ) -> AutomationCondition:
         return (
              AutomationCondition.in_latest_time_window()
              & AutomationCondition.cron_tick_passed(
                   cron_schedule, cron_timezone
              ).since_last_handled()
              & AutomationCondition.all_deps_updated_since_cron(cron_schedule, cron_timezone)
              & ~ AutomationCondition.in_progress()
         ).with_label(f"lazy_on_cron({cron_schedule}, {cron_timezone})")
    
    @staticmethod
    def eager_with_deps_checks() -> AutomationCondition:
            return ((AutomationCondition.eager()
                     & AutomationCondition.all_deps_blocking_checks_passed()))