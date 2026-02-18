from django.conf import settings
from django.db.models.signals import post_save
from django.dispatch import receiver

import dpath
import requests

from rdmo.domain.models import Attribute
from rdmo.options.models import Option
from rdmo.projects.models import Value

def get_ror_id(disambiguated_organization):
    try:
        disambiguation_source = disambiguated_organization.get('disambiguation-source')
        disambiguation_id = disambiguated_organization.get('disambiguated-organization-identifier')
    except:
        return None

    if disambiguation_source == 'ROR':
        return disambiguation_id
    
    elif disambiguation_source in ['GRID', 'FUNDREF']:
        url = getattr(settings, 'ROR_PROVIDER_URL', 'https://api.ror.org/v1/').rstrip('/')
        headers = getattr(settings, 'ROR_PROVIDER_HEADERS', {})

        response = requests.get(f'{url}/organizations?query="{disambiguation_id}"', headers=headers)

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            pass

        if data.get('number_of_results') == 1:
            ror_id = data.get('items')[0].get('id')
            return ror_id

    return None

@receiver(post_save, sender=Value)
def orcid_handler(sender, request=None, instance=None, **kwargs):
    # check for ORCID_PROVIDER_MAP
    if not getattr(settings, 'ORCID_PROVIDER_MAP', None):
        return

    # check if we are importing fixtures
    if kwargs.get('raw'):
        return

    # check if this value instance has an external_id
    if not instance.external_id:
        return

    # loop over ORCID_PROVIDER_MAP and check if the value instance attribute is found
    for attribute_map in settings.ORCID_PROVIDER_MAP:
        if 'orcid_autocomplete' in attribute_map and instance.attribute.uri == attribute_map['orcid_autocomplete']:
            # query the orcid api for the record for this orcid
            try:
                url = getattr(settings, 'ORCID_PROVIDER_URL', 'https://pub.orcid.org/v3.0/').rstrip('/')
                headers = getattr(settings, 'ORCID_PROVIDER_HEADERS', {})
                headers['Accept'] = 'application/json'

                response = requests.get(f'{url}/{instance.external_id}', headers=headers)
                response.raise_for_status()

                data = response.json()
            except (requests.exceptions.RequestException, requests.exceptions.HTTPError):
                return

            Value.objects.update_or_create(
                project=instance.project,
                attribute=Attribute.objects.get(uri='https://rdmo.mpdl.mpg.de/terms/domain/project/partner/type'),
                set_prefix=instance.set_prefix,
                set_index=instance.set_index,
                set_collection=True,
                option=Option.objects.get(uri='https://rdmo.mpdl.mpg.de/terms/options/partner-types/person')
            )
            
            for key, path in [
                ('orcid', '/orcid-identifier/uri'), 
                ('given_name', '/person/name/given-names/value'), 
                ('family_name', '/person/name/family-name/value')
            ]:
                if key in attribute_map:
                    Value.objects.update_or_create(
                        project=instance.project,
                        attribute=Attribute.objects.get(uri=attribute_map[key]),
                        set_prefix=instance.set_prefix,
                        set_index=instance.set_index,
                        defaults={
                            'text': dpath.get(data, path),
                            'set_collection': True
                        }
                    )

            if 'employment' in attribute_map:
                employments = []
                for affiliation in dpath.get(data, '/activities-summary/employments/affiliation-group'):
                    for summaries in affiliation.get('summaries'):
                        if dpath.get(summaries, '/employment-summary/end-date') is None:
                            a = dpath.get(summaries, '/employment-summary/organization/name')
                            role = dpath.get(summaries, '/employment-summary/role-title')
                            disambiguated_organization = dpath.get(summaries, '/employment-summary/organization/disambiguated-organization')
                            
                            ror_id = get_ror_id(disambiguated_organization)
                            if ror_id:
                                employments.append((role, a, ror_id))
                            else:
                                employments.append((role, None, ror_id))

                uris = [
                    'https://rdmo.mpdl.mpg.de/terms/domain/project/partner/role', 
                    'https://rdmo.mpdl.mpg.de/terms/domain/project/partner/affiliation',
                    'https://rdmo.mpdl.mpg.de/terms/domain/project/partner/affiliation/ror-id'
                ]
                for set_index, employment in enumerate(employments):
                    for i, e in enumerate(employment):
                        if e != None:
                            Value.objects.update_or_create(
                                project=instance.project,
                                attribute=Attribute.objects.get(uri=uris[i]),
                                set_prefix=instance.set_index,
                                set_index=set_index,
                                defaults={
                                    'text': e,
                                    'set_collection': True
                                }
                            )

                # delete surplus collection_indexes
                for uri in uris:
                    Value.objects.filter(
                        project=instance.project,
                        snapshot=None,
                        set_prefix=instance.set_index,
                        attribute=Attribute.objects.get(uri=uri)
                    ).exclude(set_index__in=range(len(employments))).delete()
