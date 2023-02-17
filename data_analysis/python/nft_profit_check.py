import time
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from functools import wraps

tg_host = "192.168.100.151"
tg_port = 9000
graph_name = "nft_profit"
tg_user = "data_analyst"
tg_passwd = ""

# purr_top_sql = """SELECT a.public_address
# 	,GROUP_CONCAT( concat("'",lower(coalesce(b.address,a.public_address)),"'")) as address_list
# 	,max(a.username) as username
# 	,max(a.total_profit) as total_profit
# 	,max(a.`rank`) as `rank`
# 	,max(a.rank_update_time) as rank_update_time
# FROM
# (
# 	SELECT public_address,username,total_profit,`rank`,rank_update_time
# 	FROM purr.purr_profit_snapshot
# 	WHERE `rank` < 100
# ) as a
# LEFT JOIN wiw.wallets as b
# on a.public_address COLLATE utf8mb4_unicode_ci = b.user_id
# GROUP BY 1
# ORDER BY `rank` asc"""

purr_top_list = [
    ['0xd387a6e4e84a6c86bd90c158c6028a58cc8ac459'],
    ['0x4897d38b0974051d8fa34364e37a5993f4a966a5'],
    ['0x721931508df2764fd4f70c53da646cb8aed16ace'],
    ['0xb53349160e38739b37e4bbfcf950ed26e26fcb41'],
    ['0x5be8f739c8ea94d99b44ab0b1421889c8b99b2e1'],
    ['0x63748140c409b490952c37dae5a60715bf915129'],
    ['0x46e449a3f88d0e35b4520bc36e8dfda195c896b0'],
    ['0xc142995f05436c91bba172b7d17522a301323236'],
    ['0x42de10a720c59ed8dcc6e55d5e61e03b5ad70905'],
    ['0x18799f860f9adb26e076d581603037f79342e0c0', '0x6a13f7fe7f5942de46b86c36cd8a006da8bcb38d',
     '0x7a70536c4d695b1ec9df972e91461e834bfb00e8', '0xcc6f4a34c00428248c365157381aed046383eae6'],
    ['0xa8084f1c9a21acdd1736d43a48b28fd34cf5c884', '0x39fec995c2b9776b2aa22a49f61d45af3f1a2570',
     '0xffba913bb056544b75e57312ec3eae2528c285e1', '0xfaa52baf4efd667e03233dbf44fbdb560c82358f',
     '0xf7bb42585a021cda93ef428dd328f6c9354cbab8', '0xd21e718d55c77105a52eb9a0ff3f0d2dcf74c2c6',
     '0xc69c449be66e2e0c74606d11ab11a3b791dc87f7', '0xb6c5780de66b65df84510eeee15066ddde795fd3',
     '0xacbc3094fbb1b5a51ad82f02fbb1f6206b897d20', '0xa7b5ca022774bd02842932e4358ddcbea0ccaade',
     '0x9f477d97a21389542e0a20879a3899730843dccd', '0x79d5347e03d4befb92a1374039f1998658d75d1d'],
    ['0x009988ff77eeaa00051238ee32c48f10a174933e', '0x57524efc73e194e4f19dc1606fb7a3e1dbf719b3'],
    ['0x6186290b28d511bff971631c916244a9fc539cfe'],
    ['0x2329a3412ba3e49be47274a72383839bbf1cde88'],
    ['0x7b05d42b2d6f0ca0da252fb71159ab9b299b3022', '0xb630abd9a5367763b7cba316e870c4a54064cc9f',
     '0xc665a60f22dda926b920deb8ffac0ef9d8a17460'],
    ['0x5338035c008ea8c4b850052bc8dad6a33dc2206c'],
    ['0x1ff2875cd548eeae6d22f26bae2d72a50fd143c7'],
    ['0x616ed054e0e0fdbfcad3fa2f42daed3d7d4ee448'],
    ['0x7a70536c4d695b1ec9df972e91461e834bfb00e8'],
    ['0xbdd95abe8a7694ccd77143376b0fbea183e6a740'],
    ['0x6be55c75d61ecaab7edf7791c087858269760383'],
    ['0x6be55c75d61ecaab7edf7791c087858269760383'],
    ['0xc229d7d3dd662a1b107e29aa84bb0c8ff609cf3a'],
    ['0xfa89ec40699bbfd749c4eb6643dc2b22ff0e2aa6'],
    ['0x2e0d63ffcb08ea20ff3acdbb72dfec97343885d2'],
    ['0xf8c75c5e9ec6875c57c0dbc30b59934b37908c4e'],
    ['0x3f3e2f43f0ac69f30ec38d3e4fec304bdf330e7a'],
    ['0xc665a60f22dda926b920deb8ffac0ef9d8a17460'],
    ['0x47c7d18331ccee1870f8d05371e0e1274418c08d'],
    ['0xe9090c96795f8936b3bf5c72b23d4a244cd0db13', '0x980cbfc3941c1846ac02c5019e0e26f95511fbfe',
     '0x8bcfc7a0990d3853daa69018a8e9471e0757385c', '0x7c6553933a471b43ce3a76a02245c5162c82522c'],
    ['0x325c08a558ff02f244f31ba1e87bf3c1b1c08f45'],
    ['0x336f6beca25aed6bc4b4f9e6ef5b3eb415aeca67'],
    ['0x33c2656b7b33461f346f697d998d89a110eb42ef'],
    ['0xff3879b8a363aed92a6eaba8f61f1a96a9ec3c1e'],
    ['0xa7b5ca022774bd02842932e4358ddcbea0ccaade'],
    ['0x440b4a49248f25a9cf514ad8c1557cbf504ed5c4'],
    ['0x49c3feafddaefc3bed06f4ff87ce86610c2c1076'],
    ['0xcd6e741beea10615be5b5aa8e1aac53241f4c9b1'],
    ['0x6237c88fe026b1b27f494f49ba399448aaaa93c0', '0x05e010c1bd68526576b20500289791e37c820cf8',
     '0x8bd8795cbeed15f8d5074f493c53b39c11ed37b2'],
    ['0xe35c3d08163da9bd4efa00879a78504d69820b5c'],
    ['0x61d0ea212b35721e021f56094603165a92410c19'],
    ['0x036d78c5e87e0aa07bf61815d1effe10c9fd5275'],
    ['0x5fd2c02689d138547b7b1b9e7d9a309d5a03edcd'],
    ['0x958ae6e9b3dacba32f760a2cef018c765c3d3d3c'],
    ['0x6b4331048c411795a89d54484e3653107d58a64e'],
    ['0x94ef50afac9c04572813f36fb8d676eb400de278'],
    ['0x92e9b91aa2171694d740e7066f787739ca1af9de'],
    ['0x18799f860f9adb26e076d581603037f79342e0c0'],
    ['0x7a9dc8eeaf5022cecd60c54a042343484ce6c065', '0x937879ffcc9f92a4c264d6f3db6648cb179bcd6d',
     '0xbf798b6592f8751a908edb3c1276661fa0e05aed', '0xf0cc8f80ec7ee1f3d91149ac2eb86c590b55e5c8',
     '0xfeb4d73a2daa4ef366659727b6ea6973c184b954', '0x2e0dad2eb999e8eb7e024efce3c3ec7915bc3d99',
     '0x6861698b928562e396b9b6db2c0abb17c4c729cd'],
    ['0x47c2ac06520722aaa3e32d99ec6a2352b48b1b8a'],
    ['0xe05ccac337e32ab8028903a320e911e34f12405d', '0x04cb51903a54f39abc45e3a0df9bbc9dafc5c674',
     '0xcead03574e4b930ee871bf8bb49922148a63a8e6', '0x7cae06e27196fe62c2ea57c02e29625ff1900a19'],
    ['0x76b36fa9b4bc0fd574f8abed7815f6968122c44f'],
    ['0xf29155745c8ee222d4f7d6a2a7a50c1901f27de2'],
    ['0xcf803a49f6e37b92a4040e397b976939dc5d9841'],
    ['0x7b8a6e7777793614f27799199e32e80f03d18dbe'],
    ['0x6eef09b526d883f98762a7005fabd2c800dfca44'],
    ['0xe10772c3c2e8879b13d5d2886ef8e9f9b95b83aa'],
    ['0x9274e50e3922fbc7a3ce99f94efc32d3beca6c39', '0xb585b60de71e48032e8c19b90896984afc6a660d',
     '0x13a1db3301fe0dd554aa4cd4fda4e27fa1f63bba'],
    ['0x145c01a96dcd7352a1ef00e5788c86444b369baa'],
    ['0x7abca3cbc8aa182d10f742f72e2e8bc68c4a8839'],
    ['0x08d816526bdc9d077dd685bd9fa49f58a5ab8e48'],
    ['0xa0553e045fda77dbbe741ffd5b58ae7cefdab380'],
    ['0x012e168acb9fdc90a4ed9fd6ca2834d9bf5b579e'],
    ['0x9b1acd4336ebf7656f49224d14a892566fd48e68'],
    ['0xc4293f52633b3603e65e9b4c2b4df40eeecca91c'],
    ['0xcead03574e4b930ee871bf8bb49922148a63a8e6'],
    ['0x7586834e655ee2de6357b2c8423b76efc5fbcc6b'],
    ['0x206ccba024c236dced07c35b4e9eb0bade7ef166'],
    ['0xd4fe96bb83af8b84361c7f9bfd09597e25704e30'],
    ['0x7caa9f43822e288782e3e8797c8a16774c689b3d'],
    ['0x7caa9f43822e288782e3e8797c8a16774c689b3d'],
    ['0xda6bb8c5507aff8c9ed9d787c1e8a82a0a79d629'],
    ['0xb284f19ffa703daadf6745d3c655f309d17370a5'],
    ['0xb2f40e88e28db5aa4459ac4e5a9bb8e7a4c882f2'],
    ['0x887b86b6b6957f7bbea88b8cefd392f39236a88c'],
    ['0x89601c127395d185f9b40fb53f53d5cf432d1fd1'],
    ['0x73d05c2ea70dfc3b220444c94567dbc84bb0d24c'],
    ['0xb630abd9a5367763b7cba316e870c4a54064cc9f'],
    ['0x6b611d278233cb7ca76fd5c08579c3337c01e577'],
    ['0xc477b7c1f24b94ace8d3ea62095289523471c668'],
    ['0x0676d673a2a0a13fe37a3ec7812a8ccc571ca07b'],
    ['0x57b3abae7e7392571396a4a91bc1ae35bbdd19d7'],
    ['0x57b3abae7e7392571396a4a91bc1ae35bbdd19d7'],
    ['0x7e8d6380b45d33ee8be40635484bce7c362536b2'],
    ['0x73c4818b537a4108d3e90dc83baabe80405c66d8'],
    ['0x563153823d702516f92fc24edd9358d6973f60f9'],
    ['0x6f69f79cea418024b9e0acfd18bd8de26f9bbe39'],
    ['0xe76091f84ddf27f9e773ca8bd2090830943f615c'],
    ['0xe76091f84ddf27f9e773ca8bd2090830943f615c'],
    ['0xf38598991905cce13d9cb7f8d81bca5caff9e8da'],
    ['0x00e484da1156202e9dd341ad7ea9c908bb919e96'],
    ['0xf4bdc18c46f742d1f48b84c889371f080cfd709c'],
    ['0x00e484da1156202e9dd341ad7ea9c908bb919e96'],
    ['0x244f222baa2fd3dc5d76114a82a52cdeb134a3f1', '0x6f50c303f95db19902238f26fca6cd3fed4fcb04',
     '0x8dc1ce36e06cbbf85fa28b9d4caaeb997fd708dc', '0xe672d1f51eb9c7381e01b67da9ae59d12ab04fa8',
     '0xd02d1718c2c62a5c152b27f86469b2bf2b436dc8', '0xc1f287b82919c42d4209ae63e8776a0372265007',
     '0xb310042ae3666c56f0087a0fce1e1ca3f139370c', '0xa29211e458d68a20a449d9d5b0dd25b80f199f92'],
    ['0xf9001a57d0aac84abbb7156a5825530cb163a2e0'],
    ['0x54be3a794282c030b15e43ae2bb182e14c409c5e'],
    ['0x0405049ea3757f60a43a4bdfdedf498034841fac'],
    ['0x2d3162890e0a81bb0f4a1f65e8878d20853f6216'],
    ['0x833d9fa23e20ab7c6ab836b1ebb773745b203c83', '0x5b0a56300fe42e8124e64ac273ac764571547c56',
     '0x3ba53331f1084ad49b5b13da7882165b52879ba9']
]


def with_token(fn):
    @wraps(fn)
    def wrap(self, *args, **kwargs):
        if time.time() >= self.expiration - 60:  # 提前60s认为过期
            self.request_token()
        return fn(self, *args, **kwargs)

    return wrap


class Client(object):
    def __init__(self, host: str, rest_port: int, user: str, passwd: str, graph_name: str):
        self.host = host
        self.rest_port = rest_port
        self.user = user
        self.passwd = passwd
        self.graph_name = graph_name
        self.token = "nfaqgjbn90v2gogb5u7m1c1k8pmgesgu"
        self.expiration: int = 1676098936

    def request_token(self):
        url = 'http://{}:{}/requesttoken'.format(self.host, self.rest_port)
        data = {'graph': self.graph_name}
        result = requests.post(url, json=data, auth=HTTPBasicAuth(self.user, self.passwd))
        result = result.json()
        print(result)
        self.token = result['results']['token']
        self.expiration = result['expiration']

    @with_token
    def run_query(self, query_name: str, address_list: list):
        condition_str = ""
        for address in address_list:
            if condition_str == "":
                condition_str += "address=" + address
            else:
                condition_str += "&address=" + address
        url = 'http://{}:{}/query/{}/{}?{}'.format(
            self.host, self.rest_port, self.graph_name, query_name, condition_str
        )
        headers = {
            'Authorization': 'Bearer {}'.format(self.token)
        }
        # print(url)
        result = requests.post(url, headers=headers)
        return result.json()



def nft_profit_check(input_address_list):
    c = Client(tg_host, tg_port, tg_user, tg_passwd, 'nft_profit')
    df = pd.DataFrame(columns=[
        'addresses', 'profit_prod', 'profit_test', 'run_ts_prod', 'run_ts_test'
    ])
    for address_list in input_address_list:
        start_ts_prod = time.time()
        res_prod = c.run_query('nft_profit_calculation', address_list)
        end_ts_prod = time.time()
        run_ts_prod = end_ts_prod - start_ts_prod
        profit_prod = res_prod['results'][0]['nft_cum_profit']
        try:
            start_ts_test = time.time()
            res_test = c.run_query('nft_profit_calculation_test', address_list)
            end_ts_test = time.time()
            run_ts_test = end_ts_test - start_ts_test
            profit_test = res_test['results'][0]['nft_cum_profit']
            print(address_list[0], profit_prod, profit_test, run_ts_prod, run_ts_test)
            if profit_prod != profit_test:
                for nft in res_prod['results'][1]['@@top_projects'].keys():
                    nft_profit_prod = res_prod['results'][1]['@@top_projects'][nft]
                    nft_profit_test = res_test['results'][1]['@@top_projects'][nft]
                    if nft_profit_prod != nft_profit_test:
                        print('unmatched nfts:', address_list, nft, nft_profit_prod, nft_profit_test)
        except:
            print(res_test)
            continue
        else:
            row_df = pd.DataFrame([
                {'addresses': address_list,
                 'profit_prod': profit_prod,
                 'profit_test': profit_test,
                 'run_ts_prod': run_ts_prod,
                 'run_ts_test': run_ts_test,
                 }
            ])
            df = pd.concat([df, row_df], axis=0, ignore_index=True)
            print(len(df))
    return df


if __name__ == '__main__':
    check_df = nft_profit_check(purr_top_list)
    check_df.to_excel('nft_profit_check.xlsx', sheet_name='nft_profit_check')
