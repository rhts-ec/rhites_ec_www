from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'dashboards/malaria/$', views.index, name='thematic_malaria'),
    url(r'scorecards/malaria/compliance\.php', views.malaria_compliance, {'org_unit_level': 3}, name='malaria_compliance'),
    url(r'scorecards/malaria/compliance\.xls', views.malaria_compliance, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='malaria_compliance_excel'),
    url(r'scorecards/malaria/compliance\.csv', views.malaria_compliance, {'org_unit_level': 3, 'output_format': 'CSV'}, name='malaria_compliance_csv'),
    url(r'scorecards/malaria/compliance_districts\.php', views.malaria_compliance, {'org_unit_level': 1}, name='malaria_compliance_districts'),
    url(r'scorecards/malaria/compliance_districts\.xls', views.malaria_compliance, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='malaria_compliance_districts_excel'),
    url(r'scorecards/malaria/compliance_districts\.csv', views.malaria_compliance, {'org_unit_level': 1, 'output_format': 'CSV'}, name='malaria_compliance_districts_csv'),
    url(r'scorecards/malaria/ipt_subcounties\.php', views.malaria_ipt_scorecard, {'org_unit_level': 2}, name='ipt_subcounties'),
    url(r'scorecards/malaria/ipt_subcounties\.xls', views.malaria_ipt_scorecard, {'org_unit_level': 2, 'output_format': 'EXCEL'}, name='ipt_subcounties_excel'),
    url(r'scorecards/malaria/ipt_subcounties\.csv', views.malaria_ipt_scorecard, {'org_unit_level': 2, 'output_format': 'CSV'}, name='ipt_subcounties_csv'),
    url(r'scorecards/malaria/ipt_districts\.php', views.malaria_ipt_scorecard, {'org_unit_level': 1}, name='ipt_districts'),
    url(r'scorecards/malaria/ipt_districts\.xls', views.malaria_ipt_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='ipt_districts_excel'),
    url(r'scorecards/malaria/ipt_districts\.csv', views.malaria_ipt_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='ipt_districts_csv'),
    url(r'validation/(?P<thematic_area>\w+)/rules\.php', views.validation_rule_listing, name='validation_rule_listing'),
    url(r'validation_rule\.php', views.validation_rule, name='validation_rule'),
    url(r'data_workflow_new\.php/(?P<menu_name>.*)/$', views.data_workflow_new, name='data_workflow_new'),
    url(r'validation_rule\.xls', views.validation_rule, {'output_format': 'EXCEL'}, name='validation_rule_excel'),
    url(r'data_workflow\.php', views.data_workflow_detail, name='data_workflow_detail'),
    url(r'data_workflows\.php', views.data_workflow_listing, name='data_workflow_listing'),
    url(r'data_element_alias\.php', views.data_element_alias, name='data_element_alias'),
    url(r'dashboards/hts/$', views.index, name='thematic_hts'),
    url(r'scorecards/hts/sites\.php', views.hts_scorecard, {'org_unit_level': 3}, name='hts_sites'),
    url(r'scorecards/hts/sites\.xls', views.hts_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='hts_sites_excel'),
    url(r'scorecards/hts/sites\.csv', views.hts_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='hts_sites_csv'),
    url(r'scorecards/hts/districts\.php', views.hts_scorecard, {'org_unit_level': 1}, name='hts_districts'),
    url(r'scorecards/hts/districts\.xls', views.hts_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='hts_districts_excel'),
    url(r'scorecards/hts/districts\.csv', views.hts_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='hts_districts_csv'),
    url(r'scorecards/art_new/sites\.php', views.art_new_scorecard, {'org_unit_level': 3}, name='art_new_sites'),
    url(r'scorecards/art_new/sites\.xls', views.art_new_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='art_new_sites_excel'),
    url(r'scorecards/art_new/sites\.csv', views.art_new_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='art_new_sites_csv'),
    url(r'scorecards/art_new/districts\.php', views.art_new_scorecard, {'org_unit_level': 1}, name='art_new_districts'),
    url(r'scorecards/art_new/districts\.xls', views.art_new_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='art_new_districts_excel'),
    url(r'scorecards/art_new/districts\.csv', views.art_new_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='art_new_districts_csv'),
    url(r'scorecards/art_active/sites\.php', views.art_active_scorecard, {'org_unit_level': 3}, name='art_active_sites'),
    url(r'scorecards/art_active/sites\.xls', views.art_active_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='art_active_sites_excel'),
    url(r'scorecards/art_active/sites\.csv', views.art_active_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='art_active_sites_csv'),
    url(r'scorecards/art_active/districts\.php', views.art_active_scorecard, {'org_unit_level': 1}, name='art_active_districts'),
    url(r'scorecards/art_active/districts\.xls', views.art_active_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='art_active_districts_excel'),
    url(r'scorecards/art_active/districts\.csv', views.art_active_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='art_active_districts_csv'),
    url(r'dashboards/vmmc/$', views.index, name='thematic_vmmc'),
    url(r'scorecards/vmmc/sites\.php', views.vmmc_scorecard, {'org_unit_level': 3}, name='vmmc_sites'),
    url(r'scorecards/vmmc/sites\.xls', views.vmmc_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='vmmc_sites_excel'),
    url(r'scorecards/vmmc/sites\.csv', views.vmmc_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='vmmc_sites_csv'),
    url(r'scorecards/vmmc/districts\.php', views.vmmc_scorecard, {'org_unit_level': 1}, name='vmmc_districts'),
    url(r'scorecards/vmmc/districts\.xls', views.vmmc_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='vmmc_districts_excel'),
    url(r'scorecards/vmmc/districts\.csv', views.vmmc_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='vmmc_districts_csv'),
    url(r'dashboards/lab/$', views.index, name='thematic_lab'),
    url(r'scorecards/lab/sites\.php', views.lab_scorecard, {'org_unit_level': 3}, name='lab_sites'),
    url(r'scorecards/lab/sites\.xls', views.lab_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='lab_sites_excel'),
    url(r'scorecards/lab/sites\.csv', views.lab_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='lab_sites_csv'),
    url(r'scorecards/lab/districts\.php', views.lab_scorecard, {'org_unit_level': 1}, name='lab_districts'),
    url(r'scorecards/lab/districts\.xls', views.lab_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='lab_districts_excel'),
    url(r'scorecards/lab/districts\.csv', views.lab_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='lab_districts_csv'),
    url(r'scorecards/vl/sites\.php', views.vl_scorecard, {'org_unit_level': 3}, name='vl_sites'),
    url(r'scorecards/vl/sites\.xls', views.vl_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='vl_sites_excel'),
    url(r'scorecards/vl/sites\.csv', views.vl_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='vl_sites_csv'),
    url(r'scorecards/vl/districts\.php', views.vl_scorecard, {'org_unit_level': 1}, name='vl_districts'),
    url(r'scorecards/vl/districts\.xls', views.vl_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='vl_districts_excel'),
    url(r'scorecards/vl/districts\.csv', views.vl_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='vl_districts_csv'),
    url(r'dashboards/fp/$', views.index, name='thematic_fp'),
    url(r'scorecards/fp/sites\.php', views.fp_scorecard, {'org_unit_level': 3}, name='fp_sites'),
    url(r'scorecards/fp/sites\.xls', views.fp_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='fp_sites_excel'),
    url(r'scorecards/fp/sites\.csv', views.fp_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='fp_sites_csv'),
    url(r'scorecards/fp/districts\.php', views.fp_scorecard, {'org_unit_level': 1}, name='fp_districts'),
    url(r'scorecards/fp/districts\.xls', views.fp_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='fp_districts_excel'),
    url(r'scorecards/fp/districts\.csv', views.fp_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='fp_districts_csv'),
    url(r'scorecards/fp/cyp_sites\.php', views.fp_cyp_scorecard, {'org_unit_level': 3}, name='fp_cyp_sites'),
    url(r'scorecards/fp/cyp_sites\.xls', views.fp_cyp_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='fp_cyp_sites_excel'),
    url(r'scorecards/fp/cyp_sites\.csv', views.fp_cyp_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='fp_cyp_sites_csv'),
    url(r'scorecards/fp/cyp_districts\.php', views.fp_cyp_scorecard, {'org_unit_level': 1}, name='fp_cyp_districts'),
    url(r'scorecards/fp/cyp_districts\.xls', views.fp_cyp_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='fp_cyp_districts_excel'),
    url(r'scorecards/fp/cyp_districts\.csv', views.fp_cyp_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='fp_cyp_districts_csv'),
    url(r'dashboards/tb/$', views.index, name='thematic_tb'),
    url(r'scorecards/tb/sites\.php', views.tb_scorecard, {'org_unit_level': 3}, name='tb_sites'),
    url(r'scorecards/tb/sites\.xls', views.tb_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='tb_sites_excel'),
    url(r'scorecards/tb/sites\.csv', views.tb_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='tb_sites_csv'),
    url(r'scorecards/tb/districts\.php', views.tb_scorecard, {'org_unit_level': 1}, name='tb_districts'),
    url(r'scorecards/tb/districts\.xls', views.tb_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='tb_districts_excel'),
    url(r'scorecards/tb/districts\.csv', views.tb_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='tb_districts_csv'),
    url(r'dashboards/nutrition/$', views.index, name='thematic_nutrition'),
    url(r'scorecards/nutrition/hospitals\.php', views.nutrition_by_hospital, {'org_unit_level': 3}, name='nutrition_hospitals'),
    url(r'scorecards/nutrition/hospitals\.xls', views.nutrition_by_hospital, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='nutrition_hospitals_excel'),
    url(r'scorecards/nutrition/hospitals\.csv', views.nutrition_by_hospital, {'org_unit_level': 3, 'output_format': 'CSV'}, name='nutrition_hospitals_csv'),
    url(r'dashboards/gbv/$', views.index, name='thematic_gbv'),
    url(r'scorecards/gbv/sites\.php', views.gbv_scorecard, {'org_unit_level': 3}, name='gbv_sites'),
    url(r'scorecards/gbv/sites\.xls', views.gbv_scorecard, {'org_unit_level': 3, 'output_format': 'EXCEL'}, name='gbv_sites_excel'),
    url(r'scorecards/gbv/sites\.csv', views.gbv_scorecard, {'org_unit_level': 3, 'output_format': 'CSV'}, name='gbv_sites_csv'),
    url(r'scorecards/gbv/districts\.php', views.gbv_scorecard, {'org_unit_level': 1}, name='gbv_districts'),
    url(r'scorecards/gbv/districts\.xls', views.gbv_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='gbv_districts_excel'),
    url(r'scorecards/gbv/districts\.csv', views.gbv_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='gbv_districts_csv'),
    # url(r'scorecards/gbv/pep_sites\.php', views.gbv_pep_by_site, name='gbv_pep_sites'),
    # url(r'scorecards/gbv/pep_districts\.php', views.gbv_pep_by_district, name='gbv_pep_districts'),
    url(r'dashboards/sc/$', views.index, name='thematic_sc'),
    url(r'scorecards/sc/mos_sites\.php', views.sc_mos_by_site, name='sc_mos_sites'),
    url(r'scorecards/sc/mos_sites\.xls', views.sc_mos_by_site, {'output_format': 'EXCEL'}, name='sc_mos_sites_excel'),
    url(r'scorecards/sc/mos_sites\.csv', views.sc_mos_by_site, {'output_format': 'CSV'}, name='sc_mos_sites_csv'),
    url(r'dashboards/mnch/$', views.index, name='thematic_mnch'),
    url(r'scorecards/mnch/preg_birth_subcounties\.php', views.mnch_preg_birth_scorecard, {'org_unit_level': 2}, name='mnch_preg_birth_subcounties'),
    url(r'scorecards/mnch/preg_birth_subcounties\.xls', views.mnch_preg_birth_scorecard, {'org_unit_level': 2, 'output_format': 'EXCEL'}, name='mnch_preg_birth_subcounties_excel'),
    url(r'scorecards/mnch/preg_birth_subcounties\.csv', views.mnch_preg_birth_scorecard, {'org_unit_level': 2, 'output_format': 'CSV'}, name='mnch_preg_birth_subcounties_csv'),
    url(r'scorecards/mnch/preg_birth_districts\.php', views.mnch_preg_birth_scorecard, {'org_unit_level': 1}, name='mnch_preg_birth_districts'),
    url(r'scorecards/mnch/preg_birth_districts\.xls', views.mnch_preg_birth_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='mnch_preg_birth_districts_excel'),
    url(r'scorecards/mnch/preg_birth_districts\.csv', views.mnch_preg_birth_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='mnch_preg_birth_districts_csv'),
    url(r'scorecards/mnch/pnc_child_subcounties\.php', views.mnch_pnc_child_scorecard, {'org_unit_level': 2}, name='mnch_pnc_child_subcounties'),
    url(r'scorecards/mnch/pnc_child_subcounties\.xls', views.mnch_pnc_child_scorecard, {'org_unit_level': 2, 'output_format': 'EXCEL'}, name='mnch_pnc_child_subcounties_excel'),
    url(r'scorecards/mnch/pnc_child_subcounties\.csv', views.mnch_pnc_child_scorecard, {'org_unit_level': 2, 'output_format': 'CSV'}, name='mnch_pnc_child_subcounties_csv'),
    url(r'scorecards/mnch/pnc_child_districts\.php', views.mnch_pnc_child_scorecard, {'org_unit_level': 1}, name='mnch_pnc_child_districts'),
    url(r'scorecards/mnch/pnc_child_districts\.xls', views.mnch_pnc_child_scorecard, {'org_unit_level': 1, 'output_format': 'EXCEL'}, name='mnch_pnc_child_districts_excel'),
    url(r'scorecards/mnch/pnc_child_districts\.csv', views.mnch_pnc_child_scorecard, {'org_unit_level': 1, 'output_format': 'CSV'}, name='mnch_pnc_child_districts_csv'),
]
