# Queries for merging organization data
# SQL Selects
sel_org_name = '''select name from Organizations where oid = %(oid)s'''
sel_org_concept_name = '''select name from OrgConcept where oid = %(oid)s'''
sel_lobbyemployer = '''select filer_id from LobbyistEmployer where oid = %(oid)s'''
sel_all_clients = '''select pid, assoc_name, oid, year, state from KnownClients where oid = %(bad_oid)s'''
sel_unique_clients = '''select * from KnownClients
                        where pid = %(pid)s
                        and assoc_name = %(name)s
                        and oid = %(oid)s
                        and year = %(year)s
                        and state = %(state)s'''
sel_genpub = '''select pid, did, oid from GeneralPublic where oid = %(bad_oid)s'''
sel_unique_gp = '''select * from GeneralPublic
                   where pid = %(pid)s
                   and did = %(did)s
                   and oid = %(oid)s'''
sel_lobbyingcontracts = '''select filer_id, lobbyist_employer, rpt_date, state from LobbyingContracts
                           where lobbyist_employer = %(bad_oid)s'''
sel_unique_lobbycontract = '''select * from LobbyingContracts
                              where filer_id = %(filer_id)s
                              and lobbyist_employer = %(oid)s
                              and rpt_date = %(rpt_date)s
                              and state = %(state)s'''
sel_lobbyreps = '''select pid, did, oid from LobbyistRepresentation where oid = %(bad_oid)s'''
sel_unique_lobbyrep = '''select * from LobbyistRepresentation
                         where pid = %(pid)s
                         and did = %(did)s
                         and oid = %(oid)s'''
sel_lobbydiremployment = '''select pid, lobbyist_employer, rpt_date, ls_end_yr, state from LobbyistDirectEmployment
                            where lobbyist_employer = %(bad_oid)s'''
sel_unique_lobbydiremp = '''select * from LobbyistDirectEmployment
                            where pid = %(pid)s
                            and lobbyist_employer = %(oid)s
                            and rpt_date = %(rpt_date)s
                            and ls_end_yr = %(ls_end_year)s
                            and state = %(state)s'''
sel_orgalignments = '''select oid, bid, hid, alignment, analysis_flag from OrgAlignments where oid = %(bad_oid)s'''
sel_unique_orgalignment = '''select * from OrgAlignments
                             where oid = %(oid)s
                             and bid = %(bid)s
                             and hid = %(hid)s
                             and alignment = %(alignment)s
                             and analysis_flag = %(analysis_flag)s'''

# SQL Inserts
ins_lobbyemployer = '''insert into LobbyistEmployer (oid, coalition, state)
                       select %(good_oid)s, coalition, state from LobbyistEmployer
                       where oid = %(bad_oid)s'''

# SQL Updates
up_gifts = '''update Gift set oid = %(good_oid)s where oid = %(bad_oid)s'''
up_contribution = '''update Contribution set oid = %(good_oid)s where oid = %(bad_oid)s'''
up_lobbyreps = '''update LobbyistRepresentation set oid = %(good_oid)s where oid = %(bad_oid)s'''
up_genpub = '''update GeneralPublic set oid = %(good_oid)s where oid = %(bad_oid)s'''
up_behest = '''update Behests set payee = %(good_oid)s where payee = %(bad_oid)s'''
up_combinedreps = '''update CombinedRepresentations set oid = %(good_oid)s where oid = %(bad_oid)s'''
up_knownclients = '''update KnownClients set oid = %(good_oid)s where oid = %(bad_oid)s'''
up_lobbyingcontract = '''update LobbyingContracts
                         set lobbyist_employer = %(good_oid)s
                         where lobbyist_employer = %(bad_oid)s'''
up_lobbyistdirectemployment = '''update LobbyistDirectEmployment set lobbyist_employer = %(good_oid)s
                                 where lobbyist_employer = %(bad_oid)s'''
#up_orgconcept = '''update OrgConceptAffiliation set old_oid = %(good_oid)s where old_oid = %(bad_oid)s'''
up_filer_id = '''update LobbyistEmployer set filer_id = %(filer_id)s where oid = %(oid)s'''
up_orgalignment = '''update OrgAlignments set oid = %(good_oid)s where oid = %(bad_oid)s'''

# SQL Deletes
del_gp = '''delete from GeneralPublic where pid = %(pid)s
            and did = %(did)s and oid = %(oid)s'''
del_client = '''delete from KnownClients
                where pid = %(pid)s
                and assoc_name = %(name)s
                and oid = %(oid)s
                and year = %(year)s
                and state = %(state)s'''
del_lobbycontract = '''delete from LobbyingContracts
                       where filer_id = %(filer_id)s
                       and lobbyist_employer = %(oid)s
                       and rpt_date = %(rpt_date)s
                       and state = %(state)s'''
del_lobbyrep = '''delete from LobbyistRepresentation
                  where pid = %(pid)s
                  and did = %(did)s
                  and oid = %(oid)s'''
del_unique_lobbydiremp = '''delete from LobbyistDirectEmployment
                            where pid = %(pid)s
                            and lobbyist_employer = %(oid)s
                            and rpt_date = %(rpt_date)s
                            and ls_end_yr = %(ls_end_year)s
                            and state = %(state)s'''
del_unique_orgalignment = '''delete from OrgAlignments
                             where oid = %(oid)s
                             and bid = %(bid)s
                             and hid = %(hid)s
                             and alignment = %(alignment)s
                             and analysis_flag = %(analysis_flag)s'''
del_lobbyemployer = '''delete from LobbyistEmployer where oid = %(bad_oid)s'''
del_orgconcept = '''delete from OrgConceptAffiliation where old_oid = %(bad_oid)s'''
del_orgstateaff = '''delete from OrganizationStateAffiliation where oid = %(bad_oid)s'''
del_org = '''delete from Organizations where oid = %(bad_oid)s'''

# Statements for managing OrgConcepts
sel_org_concept = '''select new_oid, old_oid from OrgConceptAffiliation where old_oid = %(bad_oid)s'''
sel_good_org_concept = '''select new_oid, old_oid from OrgConceptAffiliation where old_oid = %(good_oid)s'''
select_org_concept_id = '''select min(oid) from OrgConcept'''
#update_organization = '''update Organizations set display_flag = 0 where oid = %(bad_oid)s'''
update_organization = '''update Organizations set display_flag = 0 where oid = %(bad_oid)s'''
insert_org_concept = '''insert into OrgConcept (oid, name, canon_oid, meter_flag)
                        values (%(oid)s, %(name)s, %(canon_oid)s, 0)'''
# insert_org_concept_affiliation = '''insert into OrgConceptAffiliation (new_oid, old_oid, is_subchapter)
#                                     VALUES (%(new_oid)s, %(old_oid)s, %(is_subchapter)s)'''
insert_org_concept_affiliation = '''insert into OrgConceptAffiliation (new_oid, old_oid)
                                    VALUES (%(new_oid)s, %(old_oid)s)'''
