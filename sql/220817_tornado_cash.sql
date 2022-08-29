
/*** tornado receiver ***/

-- select to_address
-- 	,'tornado_cash_receiver' as reference_tag_name
-- 	,rank() over(PARTITION BY reference_tag_name ORDER BY nft_profit DESC) as rk
-- 	,min(transfer_time) as first_generate_time
--   ,max(transfer_time) as last_generate_time
--   ,NOW() AS etl_time
-- from dw.dwb_token_transfer_detail_eth_hi
-- where from_address in (
-- 		'0x47ce0c6ed5b0ce3d3a51fdb1c52dc66a7c3c2936',
-- 		'0x910cbd523d972eb0a6f4cae4618ad62622b39dbf',
-- 		'0xa160cdab225685da1d56aa342ad8841c3b53f291',
-- 		'0x12d66f87a04a9e220743712ce6d9bb1b5616b8fc',
-- 		'0x07687e702b410fa43f4cb4af7fa097918ffd2730',
-- 		'0x23773e65ed146a459791799d01336db287f25334',
-- 		'0x0836222f2b2b24a3f36f98668ed8f0b38d1a872f',
-- 		'0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfba9d',
-- 		'0xfd8610d20aa15b7b2e3be39b396a1bc3516c7144',
-- 		'0x169ad27a470d064dede56a2d3ff727986b15d52b',
-- 		'0xd96f2b1c14db8458374d9aca76e26c3d18364307',
-- 		'0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3',
-- 		'0xbb93e510bbcd0b7beb5a853875f9ec60275cf498',
-- 		'0x610b717796ad172b316836ac95a2ffad065ceab4',
-- 		'0x178169b423a011fff22b9e3f3abea13414ddd0f1',
-- 		'0x03893a7c7463ae47d46bc7f091665f1893656003',
-- 		'0x2717c5e28cf931547b621a5dddb772ab6a35b701',
-- 		'0xd21be7248e0197ee08e0c20d4a96debdac3d20af'
-- )
-- group by 1




/*** tornado sender ***/
select 
from 