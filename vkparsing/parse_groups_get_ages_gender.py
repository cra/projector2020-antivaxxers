import pendulum
import json

from vkparsing.vk_auth_helper import VkAuth


def main(auth: VkAuth):
    session = auth.get_session()
    vk = session.get_api()

    print(vk)

    response = vk.wall.get(count=1)  # Используем метод wall.get

    if response['items']:
        print(response['items'][0])

    for group_id in [59728906, 457918]:
        group_info = vk.groups.getById(group_id=group_id)
        members = vk.groups.getMembers(group_id=group_id)

        datestamp = pendulum.today().strftime('%Y%b%d').lower()
        fname = f'members_{group_id}_on_{datestamp}.json'
        with open(fname, 'w') as fp:
            json.dump(
                {
                    'group_info': group_info,
                    'members': members,
                },
                fp
            )
        print(f'saved {len(members)} members and group info in as', fname)


if __name__ == "__main__":
    auth = VkAuth.from_env()
    main(auth)
