rdmo-plugins-orcid (MaxIT fork - adapted for the SMP catalogue)
==================

This plugin implements dynamic option set, that queries the expanded-search endpoint of the [ORCID public API](https://info.orcid.org/documentation/api-tutorials/api-tutorial-searching-the-orcid-registry/).


Setup
-----

Install the plugin in your RDMO virtual environment using pip (directly from GitHub):

```bash
pip install git+https://github.com/MPDL/rdmo-plugins-orcid
```

Add the `rdmo_orcid` app to `INSTALLED_APPS` and the plugin to `OPTIONSET_PROVIDERS` in `config/settings/local.py`:

```python
INSTALLED_APPS += ['rdmo_orcid']

...

OPTIONSET_PROVIDERS += [
    ('orcid', _('ORCID Provider'), 'rdmo_orcid.providers.OrcidProvider')
]
```

The option set provider should now be selectable for option sets in your RDMO installation. For a minimal example catalog, see the files in `xml`.


## Settings for the SMP catalogue

```python
ORCID_PROVIDER_URL = 'https://pub.orcid.org/v3.0/'

ORCID_PROVIDER_MAP = [
    {
        'orcid_autocomplete': 'https://rdmorganiser.github.io/terms/domain/project/contributor/orcid-autocomplete',
        'orcid': 'https://rdmorganiser.github.io/terms/domain/project/contributor/orcid',
        'type': 'https://rdmorganiser.github.io/terms/domain/project/contributor/person_or_entity',
        'given_name': 'https://rdmorganiser.github.io/terms/domain/project/contributor/given_name',
        'family_name': 'https://rdmorganiser.github.io/terms/domain/project/contributor/family_name',
        'role': 'https://rdmorganiser.github.io/terms/domain/project/contributor/role',
        'organization': 'https://rdmorganiser.github.io/terms/domain/project/contributor/organization',
        'ror': 'https://rdmorganiser.github.io/terms/domain/project/contributor/organization/ror'
    }
]
```

In this case, updating the ORCID search value for a contributor (`https://rdmo.mpdl.mpg.de/terms/domain/project/partner/orcid-autocomplete`) will update their orcid, given and family name and employment values (roles and affiliations) automatically.


## General example

If a selection of a ORCIDiD should update other fields, you can add a `ORCID_PROVIDER_MAP` in your settings, e.g.:

```python
ORCID_PROVIDER_MAP = [
    {
        'orcid': 'https://rdmorganiser.github.io/terms/domain/project/dataset/creator/orcid',
        'given_name': 'https://rdmorganiser.github.io/terms/domain/project/dataset/creator/given_name',
        'family_name': 'https://rdmorganiser.github.io/terms/domain/project/dataset/creator/family_name',
        'affiliation': 'https://rdmorganiser.github.io/terms/domain/project/dataset/creator/affiliation',
    }
]
```

In this case, a change to the identifier of a coordinator (`https://rdmorganiser.github.io/terms/domain/project/dataset/creator/orcid`) will update their name (`https://rdmorganiser.github.io/terms/domain/project/dataset/creator/given_name`) automatically. `ORCID_PROVIDER_MAP` is a list of mappings, since multiple ORCIDiD could be used and should update different other values. The question for `affiliation` should be a collection since ORCID will often return more than one current affiliation.

While not required, you can add a custom `User-Agent` to your requests so that the provider can perform statistical analyses and, if you add an email address, might contact you. This can be done by adding the following to your settings.

```python
ORCID_PROVIDER_HEADERS = {
    'User-Agent': 'rdmo.example.com/1.0 (mail@rdmo.example.com) rdmo-plugins-orcid/1.0'
}
```
