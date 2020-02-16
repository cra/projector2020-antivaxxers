import vk_api
from typing import Optional
from collections import defaultdict
import json
import envparse
import pathlib
# import pandas as pd


class VkAuth:
    phone: str
    password: str
    session: Optional[vk_api.VkApi]

    def __init__(self, phone, password, session):
        self.phone: str = phone
        self.password: str = password
        self.session: Optional[vk_api.VkApi] = session

    @classmethod
    def from_env(cls):
        env = envparse.Env()
        env.read_envfile()

        return cls(
            phone=env.str('VK_PHONE'),
            password=env.str('VK_PASSWORD'),
            session=None
        )

    def get_session(self):
        if self.session is None:

            saved_config = pathlib.Path('../vk_config.v2.json')
            if saved_config.exists():
                self.session = vk_api.VkApi(self.phone)
            else:
                def auth_handler():
                    key = input('Enter authentication code')
                    return key, True

                self.session = vk_api.VkApi(
                    self.phone,
                    self.password,
                    auth_handler=auth_handler)
            self.session.auth()
        return self.session


def fetch_user_friends_for_chunk(user_ids, part, verbose=False):
    auth = VkAuth.from_env()
    session = auth.get_session()
    vk = session.get_api()

    user_id_to_groups = defaultdict(list)
    private_groups = 0
    for user_id in user_ids:
        total_fetched = 0

        try:
            group_list = vk.groups.get(
                user_id=user_id,
                filter=['groups', 'publics'],
            )
        except vk_api.ApiError:
            private_groups += 1
            continue

        total_fetched += len(group_list['items'])
        total = group_list['count']

        groups = {total_fetched: group_list['items']}
        offset = total_fetched

        while total_fetched < total:
            more_members = vk.groups.get(
                user_id=user_id,
                offset=offset,
                filter=['groups', 'publics'],
            )

            n_fetched = len(more_members['items'])
            total_fetched += n_fetched
            offset += n_fetched
            groups[total_fetched] = more_members['items']

        user_id_to_groups[user_id] = groups

    fname_out = f'user_groups_ids_part{part}.json'
    with open(fname_out, 'w') as fp:
        json.dump(
            obj={
                'private_groups': private_groups,
                'groups_by_chunks': user_id_to_groups
            },
            fp=fp
        )

    if verbose:
        print(' '.join([
            f'Total private grouplists {private_groups},'
            f'out of {len(user_ids)} total users'
        ]))
