<?php

namespace Service;

/**
 * 对redis集群的访问封装
 * 传入的hostList的格式如下
 * array(array('master'=>'192.168.1.100:6379','slave'=>array('192.168.1.101:6379','192.168.1.102:6379')),...)
 * 数组中每个元素中master只能有一台，而slave可以有多台
 * 主库读写，从库只读
 *
 * @author wentai.hqk
 */
class RedisCluster {
	/**
	 * 服务器信息
	 *
	 * @var array
	 */
	private $hostList = array ();
	/**
	 * redis实例列表
	 *
	 * @var array
	 */
	private $instanceList = array ();
	/**
	 * multi值 \Redis::MULTI或者\Redis::PIPELINE
	 *
	 * @var array
	 */
	private $multi = false;
	/**
	 * 开启了multi的redis实例
	 *
	 * @var array
	 */
	private $multiInstanceList = array ();
	/**
	 * multi中的key的调用顺序
	 *
	 * @var array
	 */
	private $multiKeyList = array ();
	/**
	 * 进入事务状态的redis实例与key的对照表
	 *
	 * @var array
	 */
	private $mapMultiHostToKey = array ();
	/**
	 *
	 * @var Flexihash
	 */
	private $oHash = NULL;
	private $queueName = "redis:cluster:queue";
	private $realkeyHashkeyMap = array ();
	public function __construct(array $hostList) {
		$this->oHash = new Flexihash ();
		$this->oHash->addTargets ( array_keys ( $hostList ) );
		$this->hostList = $hostList;
	}
	
	/**
	 * realkey为实际存储数据的key，而且所在的node，则由hashkey来决定
	 * 所有方法都首先去寻找设定的realkey=>hashkey键值对，找到决定realkey所在的节点的hashkey
	 * 此方法可以优化union inter diff等操作
	 *
	 * @since 2012-5-30 下午4:35:37
	 * @param array $map
	 *        	('key1'=>'hashkey','key2'=>'hashkey',...)
	 * @return void
	 */
	public function setRealKeyHashKeyMap(array $map) {
		$this->realkeyHashkeyMap = $map;
		return $this;
	}
	
	/**
	 *
	 * @param string $key        	
	 * @return mixed return string on successed, false on failed
	 */
	public function get($key) {
		return $this->getRedis ( $key, 'slave' )->get ( $key );
	}
	/**
	 *
	 * @param string $key        	
	 * @param string $value        	
	 * @param integer $timeout        	
	 * @return boolean
	 */
	public function set($key, $value, $timeout = false) {
		return $this->getRedis ( $key )->set ( $key, $value, $timeout );
	}
	/**
	 *
	 * @param string $key        	
	 * @param integer $ttl        	
	 * @param string $value        	
	 * @return boolean
	 */
	public function setex($key, $ttl, $value) {
		return $this->getRedis ( $key )->setex ( $key, $ttl, $value );
	}
	/**
	 * 2012-5-29 下午4:49:42
	 * //since redis 2.6
	 *
	 * @param string $key        	
	 * @param integer $ttl        	
	 * @param string $value        	
	 * @return boolean
	 */
	public function psetex($key, $ttl, $value) {
		return $this->getRedis ( $key )->psetex ( $key, $ttl, $value );
	}
	/**
	 *
	 * @since 2012-5-29 下午5:02:31
	 * @param string $key        	
	 * @param string $value        	
	 * @return boolean
	 */
	public function setnx($key, $value) {
		return $this->getRedis ( $key )->setnx ( $key, $value );
	}
	public function del(array $keyList) {
		$this->multi ( \Redis::PIPELINE );
		foreach ( $keyList as $key ) {
			$this->getRedis ( $key )->del ( $key );
		}
		return $this->exec ();
	}
	public function delete(array $keyList) {
		return $this->del ( $keyList );
	}
	public function getSet($key, $value) {
		return $this->getRedis ( $key )->getSet ( $key, $value );
	}
	public function append($key, $value) {
		return $this->getRedis ( $key )->append ( $key, $value );
	}
	public function getRange($key, $start, $end) {
		return $this->getRedis ( $key, 'slave' )->getRange ( $key, $start, $end );
	}
	public function setRange($key, $offset, $value) {
		return $this->getRedis ( $key )->setRange ( $key, $offset, $value );
	}
	public function strlen($key) {
		return $this->getRedis ( $key, 'slave' )->strlen ( $key );
	}
	public function getBit($key, $offset) {
		return $this->getRedis ( $key, 'slave' )->getBit ( $key, $offset );
	}
	public function setBit($key, $offset, $value) {
		return $this->getRedis ( $key )->setBit ( $key, $offset, $value );
	}
	/**
	 * 使用pipeline实现，请勿再开启pipeline
	 *
	 * @param array $map
	 *        	key=>value 键值对
	 */
	public function mset(array $map) {
		$this->multi ( \Redis::PIPELINE );
		foreach ( $map as $key => $value ) {
			$this->getRedis ( $key )->set ( $key, $value );
		}
		return $this->exec ();
	}
	public function msetnx(array $map) {
		$this->multi ( \Redis::PIPELINE );
		foreach ( $map as $key => $value ) {
			$this->getRedis ( $key )->setnx ( $key, $value );
		}
		return $this->exec ();
	}
	
	/**
	 * since redis 2.6
	 * 在redis cluster
	 * 发布之前，请使用setRealKeyHashKeyMap方法设定需要bitop的keys所使用的hashkey，以确定所有需要bitop的key都在同一个节点上
	 *
	 * @param string $operation
	 *        	either "AND", "OR", "NOT", "XOR"
	 * @param string $dstKey        	
	 * @param array $keyList        	
	 */
	public function bitop($operation, $dstKey, array $keyList) {
		$redis = $this->getRedis ( $dstKey );
		array_unshift ( $keyList, $dstKey );
		return call_user_func_array ( array (
				$redis,
				'bitop' 
		), $keyList );
	}
	// since redis 2.6
	public function bitcount($key) {
		return $this->getRedis ( $key, 'slave' )->bitcount ( $key );
	}
	
	/**
	 * multi exec discard 在调用multi时，如果传入Redis::MULTI，所有实例都将进入事务状态，
	 * 如果传入Redis::PIPELINE,则所有的实例都进入pipeline状态
	 *
	 * @param integer $mode        	
	 */
	public function multi($mode = \Redis::MULTI) {
		$this->multi = $mode;
		return $this;
	}
	public function exec() {
		if (! $this->multi) {
			return false;
		}
		$r = array ();
		foreach ( $this->multiInstanceList as $hostInfo => $instance ) {
			$t = $instance->exec ();
			$count = count ( $t );
			// 映射调用顺序
			for($i = 0; $i < $count; $i ++) {
				$key = $this->mapMultiHostToKey [$hostInfo] [$i];
				$index = array_search ( $key, $this->multiKeyList );
				unset ( $this->multiKeyList [$index] );
				$r [$index] = $t [$i];
			}
			unset ( $t, $count );
		}
		// reset state
		
		$this->multi = false;
		$this->multiInstanceList = array ();
		ksort ( $r );
		return $r;
	}
	public function discard() {
		if (! $this->multi) {
			return false;
		}
		foreach ( $this->multiInstanceList as $instance ) {
			$instance->discard ();
		}
		
		$this->multi = false;
		$this->multiInstanceList = array ();
		return $this;
	}
	public function watch(array $keyList) {
		foreach ( $keyList as $key ) {
			$this->getRedis ( $key )->watch ( $key );
			unset ( $key );
		}
	}
	public function unwatch() {
		foreach ( $this->instanceList as $instance ) {
			$instance->unwatch ();
			unset ( $instance );
		}
	}
	
	/**
	 *
	 * @since 2012-6-5 上午9:31:30
	 * @param array $channel        	
	 * @param mixed $callback        	
	 * @throws \Exception
	 * @return return_type
	 */
	public function subscribe(array $channel, $callback) {
		throw new \Exception ( 'not implemented' );
	}
	public function publish($channel, $message) {
		throw new \Exception ( 'not implemented' );
	}
	public function exists($key) {
		return $this->getRedis ( $key, 'slave' )->exists ( $key );
	}
	public function incr($key) {
		return $this->getRedis ( $key )->incr ( $key );
	}
	public function incrBy($key, $value) {
		return $this->getRedis ( $key )->incrBy ( $key, $value );
	}
	public function incrByFloat($key, $value) {
		return $this->getRedis ( $key )->incrByFloat ( $key, $value );
	}
	public function decr($key) {
		return $this->getRedis ( $key )->decr ( $key );
	}
	public function decrBy($key, $value) {
		return $this->getRedis ( $key )->decrBy ( $key, $value );
	}
	public function mGet(array $keyList) {
		$this->multi ( \Redis::PIPELINE );
		foreach ( $keyList as $key ) {
			$this->getRedis ( $key, 'slave' )->get ( $key );
		}
		return $this->exec ();
	}
	public function getMultiple(array $keyList) {
		return $this->mGet ( $keyList );
	}
	
	// 如果不在同一个实例，会对srcKey调用rpop，对dstKey调用lpush 此时的操作是非原子性的
	public function rpoplpush($srcKey, $dstKey) {
		if ($this->oHash->lookup ( $this->getHashKey ( $srcKey ) ) == $this->oHash->lookup ( $this->getHashKey ( $dstKey ) )) {
			return $this->getRedis ( $srcKey )->rpoplpush ( $srcKey, $dstKey );
		} else {
			$el = $this->rPop ( $srcKey );
			$this->lPush ( $dstKey, $el );
			return $el;
		}
	}
	
	// 如果不在同一个实例，会对srcKey调用brpop，对dstKey调用lpush 此时的操作是非原子性的
	public function brpoplpush($srcKey, $dstKey, $timeout) {
		if ($this->oHash->lookup ( $this->getHashKey ( $srcKey ) ) == $this->oHash->lookup ( $this->getHashKey ( $dstKey ) )) {
			return $this->getRedis ( $srcKey )->brpoplpush ( $srcKey, $dstKey, $timeout );
		} else {
			$el = $this->brPop ( $srcKey, $timeout );
			$this->lPush ( $dstKey, $el );
			return $el;
		}
	}
	// lists
	public function lPush($key, $value) {
		if ($this->getRedis ( $key )->lPush ( $key, $value )) {
			return $this->push ( $key );
		}
		return false;
	}
	public function rPush($key, $value) {
		if ($this->getRedis ( $key )->rPush ( $key, $value )) {
			return $this->push ( $key );
		}
		return false;
	}
	public function lPushx($key, $value) {
		$this->getRedis ( $key )->lPushx ( $key, $value );
	}
	public function rPushx($key, $value) {
		return $this->getRedis ( $key )->rPushx ( $key, $value );
	}
	public function lPop($key) {
		return $this->getRedis ( $key )->lPop ( $key );
	}
	public function rPop($key) {
		return $this->getRedis ( $key )->rPop ( $key );
	}
	//
	public function blPop(array $keyList, $timeout) {
		$key = $this->bpop ( $timeout );
		if (in_array ( $key, $keyList )) {
			return $this->getRedis ( $key )->lPop ( $key );
		}
		return false;
	}
	public function brPop(array $keyList, $timeout) {
		$key = $this->bpop ( $timeout );
		if (in_array ( $key, $keyList )) {
			return $this->getRedis ( $key )->rPop ( $key );
		}
		return false;
	}
	public function lSize($key) {
		return $this->getRedis ( $key, 'slave' )->lSize ( $key );
	}
	public function lIndex($key, $index) {
		return $this->getRedis ( $key, 'slave' )->lIndex ( $key, $index );
	}
	public function lGet($key, $index) {
		return $this->lIndex ( $key, $index );
	}
	public function lSet($key, $index, $value) {
		return $this->getRedis ( $key )->lSet ( $key, $index, $value );
	}
	public function lRange($key, $start, $end) {
		return $this->getRedis ( $key, 'slave' )->lRange ( $key, $start, $end );
	}
	public function lGetRange($key, $start, $end) {
		return $this->lRange ( $key, $start, $end );
	}
	public function lTrim($key, $start, $end) {
		return $this->getRedis ( $key )->lTrim ( $key, $start, $end );
	}
	public function listTrim($key, $start, $end) {
		return $this->lTrim ( $key, $start, $end );
	}
	public function lRem($key, $value, $count) {
		return $this->getRedis ( $key )->lRem ( $key, $value, $count );
	}
	public function lRemove($key, $value, $count) {
		return $this->lRem ( $key, $value, $count );
	}
	/**
	 *
	 * @param string $key        	
	 * @param integer $position
	 *        	Redis::BEFORE | Redis::AFTER
	 * @param mixed $pivot        	
	 * @param mixed $value        	
	 */
	public function lInsert($key, $position, $pivot, $value) {
		return $this->getRedis ( $key )->lInsert ( $key, $position, $pivot, $value );
	}
	
	// set
	public function sAdd($key, $value) {
		return $this->getRedis ( $key )->sAdd ( $key, $value );
	}
	public function sRem($key, $member) {
		return $this->getRedis ( $key )->sRem ( $key, $member );
	}
	public function sRemove($key, $member) {
		return $this->sRem ( $key, $member );
	}
	// 如果srcKey和dstKey在同一个node上，则操作是原子的。
	public function sMove($srcKey, $dstKey, $member) {
		if ($this->oHash->lookup ( $this->getHashKey ( $srcKey ) ) == $this->oHash->lookup ( $this->getHashKey ( $dstKey ) )) {
			$this->getRedis ( $srcKey )->sMove ( $srcKey, $dstKey, $member );
		} else {
			if (! $this->sIsMember ( $srcKey, $member )) {
				return 0;
			}
			$this->sRem ( $srcKey, $member );
			return $this->sAdd ( $dstKey, $member );
		}
	}
	public function sIsMember($key, $value) {
		return $this->getRedis ( $key, 'slave' )->sIsMember ( $key, $value );
	}
	public function sContains($key, $value) {
		return $this->sIsMember ( $key, $value );
	}
	public function sCard($key) {
		return $this->getRedis ( $key, 'slave' )->sCard ( $key );
	}
	public function sSize($key) {
		return $this->sCard ( $key );
	}
	public function sPop($key) {
		return $this->getRedis ( $key )->sPop ( $key );
	}
	public function sRandMember($key) {
		return $this->getRedis ( $key )->sRandMember ( $key );
	}
	
	// 如果keyList中的key不在同一个实例中，则取出来在客户端进行操作，再存储到服务器，此时操作是非原子的
	public function sInter(array $keyList) {
		return $this->sIntegerUnion ( $keyList );
	}
	public function sInterStore($dstKey, array $keyList) {
		return $this->sInterUnionStore ( $dstKey, $keyList );
	}
	public function sUnion(array $keyList) {
		return $this->sIntegerUnion ( $keyList, 'sUnion' );
	}
	public function sUnionStore($dstKey, array $keyList) {
		return $this->sInterUnionStore ( $dstKey, $keyList, 'sUnionStore' );
	}
	/**
	 * @since 2012-6-13  下午1:11:46
	 * @param unknown_type $dstKey
	 * @param array $keyList
	 * @param unknown_type $cmd
	 * @return mixed|boolean|number
	 */
	private function sInterUnionStore($dstKey, array $keyList, $cmd = 'sInterStore') {
		$groupList = array ();
		$groupList [$this->oHash->lookup ( $this->getHashKey ( $dstKey ) )] = array (
				$dstKey 
		);
		
		foreach ( $keyList as $key ) {
			$node = $this->oHash->lookup ( $this->getHashKey ( $key ) );
			if (! isset ( $groupList [$node] )) {
				$groupList [$node] = array ();
			}
			
			$groupList [$node] [] = $key;
			unset ( $key );
		}
		
		// 所有key在同一个实例中
		if (count ( $groupList ) == 1) {
			$o = $this->getRedis ( $dstKey );
			array_unshift ( $keyList, $dstKey );
			return call_user_func_array ( array (
					$o,
					$cmd 
			), $keyList );
		} else { // 不在同一个实例中
			$cmdFor = substr ( $cmd, 0, strpos ( $cmd, 'Store' ) );
			$r = $this->$cmdFor ( $keyList );
			if (! $r) {
				return false;
			}
			$this->multi ( \Redis::PIPELINE );
			foreach ( $r as $v ) {
				$this->sAdd ( $dstKey, $v );
				unset ( $v );
			}
			return array_sum ( $this->exec () );
		}
	}
	private function sIntegerUnion($keyList, $cmd = 'sInter') {
		$groupList = array ();
		foreach ( $keyList as $key ) {
			$node = $this->oHash->lookup ( $this->getHashKey ( $key ) );
			if (! isset ( $groupList [$node] )) {
				$groupList [$node] = array ();
			}
			$groupList [$node] [] = $key;
			unset ( $key );
		}
		// 都在一个实例上
		if (count ( $groupList ) == 1) {
			return $this->getRedis ( $keyList [0], 'slave' )->$cmd ( $keyList );
		} else {
			$r = array ();
			$first = true;
			foreach ( $groupList as $node => $v ) {
				$result = $this->getRedis ( $v [0], 'slave' )->$cmd ( $v );
				if (! $result) {
					break;
				}
				$r [] = $result;
			}
			
			if (count ( $r ) == 1) { // 只有一个有结果
				return $r;
			}
			if ($cmd == 'sInter') {
				return call_user_func_array ( 'array_intersect', $r );
			} else if ($cmd == 'sUnion') {
				return array_unique ( call_user_func_array ( 'array_merge', $r ) );
			} else {
				return call_user_func_array ( 'array_diff', $r );
			}
		}
	}
	public function sDiff(array $keyList) {
		return $this->sIntegerUnion ( $keyList, 'sDiff' );
	}
	public function sDiffStore($dstKey, array $keyList) {
		return $this->sInterUnionStore ( $dstKey, $keyList, 'sDiffStore' );
	}
	public function sMembers($key) {
		return $this->getRedis ( $key, 'slave' )->sMembers ( $key );
	}
	public function sGetMembers($key) {
		return $this->sGetMembers ( $key );
	}
	
	// keys 将会从集群中任意一个实例返回一个随机的key
	public function randomKey() {
		$key = "random:key:" . rand ( 0, 100000000 ); //
		return $this->getRedis ( $key, 'slave' )->randomKey ();
	}
	// since 2.6
	public function rename($srcKey, $dstKey) {
		return $this->renameCommon ( 'rename', $srcKey, $dstKey );
	}
	public function renameKey($srcKey, $dstKey) {
		return $this->rename ( $srcKey, $dstKey );
	}
	public function renameNx($srcKey, $dstKey) {
		return $this->renameCommon ( 'renameNx', $srcKey, $dstKey );
	}
	private function renameCommon($cmd, $srcKey, $dstKey) {
		$dstNode = $this->oHash->lookup ( $this->getHashKey ( $dstKey ) );
		$srcNode = $this->oHash->lookup ( $this->getHashKey ( $srcKey ) );
		if ($dstNode == $srcNode) {
			return $this->getRedis ( $srcKey )->$cmd ( $srcKey, $dstKey );
		} else { // 首先将src转移到dst节点，然后改变名字
			if ($this->pmigrate ( $dstNode, $srcKey, 5 )) {
				return $this->getRedis ( $dstKey )->$cmd ( $srcKey, $dstKey );
			}
			return false;
		}
	}
	public function setTimeout($key, $ttl) {
		return $this->getRedis ( $key )->setTimeout ( $key, $ttl );
	}
	public function expire($key, $ttl) {
		return $this->setTimeout ( $key, $ttl );
	}
	// pexpire的ttl的单位是毫秒 1/1000秒 since redis 2.6
	public function pexpire($key, $ttl) {
		return $this->getRedis ( $key )->pexpire ( $key, $ttl );
	}
	public function expireAt($key, $timestamp) {
		return $this->getRedis ( $key )->expireAt ( $key, $timestamp );
	}
	// since redis 2.6
	public function pexpireAt($key, $timestamp) {
		return $this->getRedis ( $key )->pexpireAt ( $key, $timestamp );
	}
	// 返回格式为('host:port'=>array(key1,key2,...),...)
	public function keys($pattern) {
		$r = array ();
		$this->getAllInstance ();
		foreach ( $this->instanceList as $hostInfo => $instance ) {
			$r [$hostInfo] = $instance->keys ( $pattern );
		}
		return $r;
	}
	public function getKeys($pattern) {
		return $this->keys ( $pattern );
	}
	// $retrieve的取值encoding refcount idletime
	public function object($retrieve, $key) {
		return $this->getRedis ( $key, 'slave' )->object ( $retrieve, $key );
	}
	/**
	 *
	 * @param string $key
	 *        	string: Redis::REDIS_STRING
	 *        	set: Redis::REDIS_SET
	 *        	list: Redis::REDIS_LIST
	 *        	zset: Redis::REDIS_ZSET
	 *        	hash: Redis::REDIS_HASH
	 *        	other: Redis::REDIS_NOT_FOUND
	 */
	public function type($key) {
		return $this->getRedis ( $key, 'slave' )->type ( $key );
	}
	
	// connection
	/**
	 * 返回格式为('host:port'=>512,...)单位是byte
	 * 2012-5-29 下午4:53:14
	 *
	 * @return multitype:NULL
	 * @return mixed
	 */
	public function dbSize() {
		$r = array ();
		$this->getAllInstance ();
		foreach ( $this->instanceList as $hostInfo => $instance ) {
			$r [$hostInfo] = $instance->dbSize ();
		}
		return $r;
	}
	// 返回格式为('host:port'=>unixtimestamp,...)
	public function lastSave() {
		$r = array ();
		$this->getAllInstance ();
		foreach ( $this->instanceList as $hostInfo => $instance ) {
			$r [$hostInfo] = $instance->lastSave ();
		}
		return $r;
	}
	/**
	 *
	 * @param string $key        	
	 * @param array $options
	 *        	'by' => 'some_pattern_*', 不支持
	 *        	'limit' => array(0, 1),
	 *        	'get' => 'some_other_pattern_*' or an array of patterns, 不支持
	 *        	'sort' => 'asc' or 'desc',
	 *        	'alpha' => TRUE,
	 *        	'store' => 'external-key' 需要redis server 2.6以上
	 */
	public function sort($key, array $options = array()) {
		$storeKey = isset ( $options ['store'] ) ? $options ['store'] : '';
		if (! $storeKey) {
			return $this->getRedis ( $key, 'slave' )->sort ( $key, $options );
		} else {
			// 在同一节点上
			if ($this->oHash->lookup ( $this->getHashKey ( $storeKey ) ) == $this->oHash->lookup ( $this->getHashKey ( $key ) )) {
				return $this->getRedis ( $key )->sort ( $key, $options );
			} else { // 需要redis 2.6以上支持 不在同一节点上，首先排序存储，然后移动到目标实例
				$r = $this->getRedis ( $key )->sort ( $key, $options );
				if ($r) {
					return $this->pmigrate ( $this->oHash->lookup ( $storeKey ), $key, 5 );
				}
				return false;
			}
		}
	}
	// 返回格式 array('host:port'=>array(...),...)
	public function info($option = '') {
		$r = array ();
		$this->getAllInstance ();
		foreach ( $this->instanceList as $hostInfo => $instance ) {
			$r [$hostInfo] = $instance->info ( $option );
		}
		return $r;
	}
	// 重置所有服务器的状态
	public function resetStat() {
		$r = array ();
		$this->getAllInstance ();
		foreach ( $this->instanceList as $hostInfo => $instance ) {
			$r [$hostInfo] = $instance->resetStat ();
		}
		return $r;
	}
	public function ttl($key) {
		return $this->getRedis ( $key, 'slave' )->ttl ( $key );
	}
	public function pttl($key) {
		return $this->getRedis ( $key, 'slave' )->pttl ( $key );
	}
	public function persist($key) {
		return $this->getRedis ( $key )->persist ( $key );
	}
	
	// zset
	public function zAdd($key, $score, $value) {
		return $this->getRedis ( $key )->zAdd ( $key, $score, $value );
	}
	public function zRange($key, $start, $end, $withscore = false) {
		return $this->getRedis ( $key, 'slave' )->zRange ( $key, $start, $end, $withscore );
	}
	public function zRevRange($key, $start, $end, $withscore = false) {
		return $this->getRedis ( $key, 'slave' )->zRevRange ( $key, $start, $end, $withscore );
	}
	public function zDelete($key, $member) {
		return $this->getRedis ( $key )->zDelete ( $key, $member );
	}
	public function zRem($key, $member) {
		return $this->zDelete ( $key, $member );
	}
	// Two options are available: withscores => TRUE, and limit =>
	// array($offset, $count)
	public function zRangeByScore($key, $start, $end, array $options = array()) {
		return $this->getRedis ( $key, 'slave' )->zRangeByScore ( $key, $start, $end, $options );
	}
	public function zRevRangeByScore($key, $start, $end, array $options = array()) {
		return $this->getRedis ( $key, 'slave' )->zRevRangeByScore ( $key, $start, $end, $options );
	}
	public function zCount($key, $start, $end) {
		return $this->getRedis ( $key, 'slave' )->zCount ( $key, $start, $end );
	}
	public function zRemRangeByScore($key, $start, $end) {
		return $this->getRedis ( $key )->zRemRangeByScore ( $key, $start, $end );
	}
	public function zDeleteRangeByScore($key, $start, $end) {
		return $this->zRemRangeByScore ( $key, $start, $end );
	}
	public function zRemRangeByRank($key, $start, $end) {
		return $this->getRedis ( $key )->zRemRangeByRank ( $key, $start, $end );
	}
	public function zDeleteRangeByRank($key, $start, $end) {
		return $this->zRemRangeByRank ( $key, $start, $end );
	}
	public function zSize($key) {
		return $this->getRedis ( $key, 'slave' )->zSize ( $key );
	}
	public function zCard($key) {
		return $this->zSize ( $key );
	}
	public function zScore($key, $member) {
		return $this->getRedis ( $key, 'slave' )->zScore ( $key, $member );
	}
	public function zRank($key, $member) {
		return $this->getRedis ( $key, 'slave' )->zRank ( $key, $member );
	}
	public function zRevRank($key, $member) {
		return $this->getRedis ( $key, 'slave' )->zRevRank ( $key, $member );
	}
	public function zIncrBy($key, $value, $member) {
		return $this->getRedis ( $key )->zIncrBy ( $key, $value, $member );
	}
	/**
	 *
	 * @param string $dstKey        	
	 * @param array $keyList        	
	 * @param array $weights        	
	 * @param string $aggregateFunction
	 *        	SUM MIN MAX
	 */
	public function zUnion($dstKey, array $keyList, array $weights = array(), $aggregateFunction = 'SUM') {
		return $this->zUnionInterCommon ( 'zUnion', $dstKey, $keyList, $weights, $aggregateFunction );
	}
	/**
	 *
	 * @param string $dstKey        	
	 * @param array $keyList        	
	 * @param array $weights        	
	 * @param string $aggregateFunction
	 *        	SUM MIN MAX
	 */
	public function zInter($dstKey, array $keyList, array $weights = array(), $aggregateFunction = 'SUM') {
		return $this->zUnionInterCommon ( 'zInter', $dstKey, $keyList, $weights, $aggregateFunction );
	}
	private function zUnionInterCommon($cmd, $dstKey, array $keyList, array $weights = array(), $aggregateFunction = 'SUM') {
		$computeKeynode = $this->oHash->lookup ( $this->getHashKey ( $dstKey ) ); // 在dstKey所在节点进行计算
		$tmpKeyRealKeyMap = array (); // keyList中的每个key都对应一个临时的key
		                              // 1.将与dstKey不在同一个节点上的key转移到$computeKeynode
		foreach ( $keyList as $key ) {
			// 如果$key不在$computeKeynode上，则将$key改名，并将该名后的$key传送到$computeKeynode
			$keyNode = $this->oHash->lookup ( $this->getHashKey ( $key ) );
			$tmpKey = $key;
			if ($keyNode != $computeKeynode) {
				// 1.将key改为另外一个key_tmp
				// 2.将key_tmp转移进入dstKeynode
				$tmpKey = $key . ":for:zunion:tmp";
				$this->rename ( $key, $tmpKey );
				$this->pmigrate ( $computeKeynode, $tmpKey, 30 );
			}
			$tmpKeyRealKeyMap [$key] = $tmpKey;
			unset ( $keyNode, $tmpKey );
		}
		
		// 2.进行操作
		$this->getRedis ( $dstKey )->$cmd ( $dstKey, array_values ( $tmpKeyRealKeyMap ), $weights, $aggregateFunction );
		
		// 3.将keylist中的key回传到原来的节点
		foreach ( $tmpKeyRealKeyMap as $key => $tmpKey ) {
			if ($key == $tmpKey) { // 如果key名没有改变，则说明没有转移
				continue;
			}
			$origkeyNode = $this->oHash->lookup ( $this->getHashKey ( $key ) ); // 找到源node
			$this->pmigrate ( $origkeyNode, $tmpKey, 30, $computeKeynode ); // 将key从计算node传送回源node
			$this->getRedis ( $key )->$cmd ( $key, array (
					$tmpKey 
			), array (
					1,
					1 
			), $aggregateFunction );
			$this->getRedis ( $key )->delete ( $key );
		}
		return $this->getRedis ( $dstKey, 'slave' )->zCard ( $dstKey );
	}
	
	// hashtables
	/**
	 *
	 * @param string $key        	
	 * @param string $hashKey        	
	 * @param string $value        	
	 * @return 1 if value didn't exist and was added successfully, 0 if the
	 *         value was already present and was replaced, FALSE if there was an
	 *         error.
	 */
	public function hSet($key, $hashKey, $value) {
		return $this->getRedis ( $key )->hSet ( $key, $hashKey, $value );
	}
	public function hSetNx($key, $hashKey, $value) {
		return $this->getRedis ( $key )->hSetNx ( $key, $hashKey, $value );
	}
	public function hGet($key, $hashKey) {
		return $this->getRedis ( $key, 'slave' )->hGet ( $key, $hashKey );
	}
	public function hLen($key) {
		return $this->getRedis ( $key, 'slave' )->hLen ( $key );
	}
	public function hDel($key, $hashKey) {
		return $this->getRedis ( $key )->hDel ( $key, $hashKey );
	}
	public function hKeys($key) {
		return $this->getRedis ( $key, 'slave' )->hKeys ( $key );
	}
	public function hVals($key) {
		return $this->getRedis ( $key, 'slave' )->hVals ( $key );
	}
	public function hGetAll($key) {
		return $this->getRedis ( $key, 'slave' )->hGetAll ( $key );
	}
	public function hExists($key, $memberKey) {
		return $this->getRedis ( $key, 'slave' )->hExists ( $key, $memberKey );
	}
	public function hIncrBy($key, $member, $value) {
		return $this->getRedis ( $key )->hIncrBy ( $key, $member, $value );
	}
	public function hIncrByFloat($key, $member, $value) {
		return $this->getRedis ( $key )->hIncrByFloat ( $key, $member, $value );
	}
	public function hMset($key, array $map) {
		return $this->getRedis ( $key )->hMset ( $key, $map );
	}
	public function hMGet($key, array $memberList) {
		return $this->getRedis ( $key, 'slave' )->hMGet ( $key, $memberList );
	}
	/**
	 *
	 * @param string $operation        	
	 * @param string $key        	
	 * @param string $value        	
	 */
	public function config($operation, $key, $value = '') {
		$this->getAllInstance ();
		$r = array ();
		foreach ( $this->instanceList as $hostInfo => $instance ) {
			if ($operation == 'set') {
				$r [$hostInfo] = $instance->config ( 'set', $key, $value );
			} else {
				$r [$hostInfo] = $instance->config ( 'get', $key );
			}
		}
		return $r;
	}
	public function configGet($key) {
		return $this->config ( 'get', $key );
	}
	public function configSet($key, $value) {
		return $this->config ( 'set', $key, $value );
	}
	public function evalLua($luaString, array $argv, $keyNum = 0) {
		throw new \Exception ( 'not implemented' );
	}
	public function evalSha($luaStringSha, array $argv, $keyNum = 0) {
		throw new \Exception ( 'not implemented' );
	}
	public function script($cmd, array $srcipt = array()) {
		throw new \Exception ( 'not implemented' );
	}
	public function getLastError() {
		throw new \Exception ( 'not implemented' );
	}
	public function _prefix($key) {
		throw new \Exception ( 'not implemented' );
	}
	public function _unserialize($value) {
		throw new \Exception ( 'not implemented' );
	}
	public function dump($key) {
		return $this->getRedis($key,'slave')->dump($key);
	}
	public function restore($key, $ttl, $bValue) {
		return $this->getRedis($key)->restore($key, $ttl, $bValue);
	}
	private function pmigrate($hostKey, $key, $timeout, $orighostKey = NULL) {
		$hostInfo = $this->hostList [$hostKey];
		$hostInfo = $hostInfo ['master'];
		$hostInfo = explode ( ':', $hostInfo );
		if ($orighostKey !== NULL) {
			$orighostInfo = $this->hostList [$orighostKey];
			$orighostInfo = $orighostInfo ['master'];
			$o = $this->getInstance ( $orighostInfo );
		} else {
			$o = $this->getRedis ( $key );
		}
		return $o->migrate ( $hostInfo [0], $hostInfo [1], $key, 0, $timeout );
	}
	
	// $type master for write and read, slave only for read
	private function getRedis($key, $type = 'master') {
		// 查找$key所对应的hashkey
		$key = $this->getHashKey ( $key );
		// 通过oHash找到所在节点
		$keyNode = $this->oHash->lookup ( $key );
		if (! isset ( $this->hostList [$keyNode] )) {
			throw new \Exception ( 'not found the key:' . $keyNode . ' in hostlist' );
		}
		// 找到节点的ip port信息
		$hostInfo = $this->hostList [$keyNode];
		$hostInfo = isset ( $hostInfo [$type] ) ? $hostInfo [$type] : '';
		// 如果找不到slave，则找master
		if ($type == 'slave') {
			if (! $hostInfo) {
				$hostInfo = isset ( $hostInfo ['master'] ) ? $hostInfo ['master'] : '';
			} else { // 随机从所有slave中抽取一台作为访问
				$hostInfo = $hostInfo [array_rand ( $hostInfo )];
			}
		}
		if (! $hostInfo) {
			throw new \Exception ( 'not found redis host' );
		}
		
		if (! $this->getInstance ( $hostInfo )) {
			throw new \Exception ( 'can\'t connect to ' . $hostInfo );
		}
		// 如果开启了multi模式，则将开启事务的实例放入另外一个sets
		if ($this->multi) {
			if (! isset ( $this->multiInstanceList [$hostInfo] )) {
				$this->multiInstanceList [$hostInfo] = $this->instanceList [$hostInfo]->multi ( $this->multi );
			}
			$this->multiKeyList [] = $key; // 保存对该key的调用顺序，不支持对单个key的多次调用
			if (! isset ( $this->mapMultiHostToKey [$hostInfo] )) {
				$this->mapMultiHostToKey [$hostInfo] = array ();
			}
			$this->mapMultiHostToKey [$hostInfo] [] = $key;
		}
		
		return $this->instanceList [$hostInfo];
	}
	private function getAllMasterInstance() {
		return $this->getAllInstance ( 'master' );
	}
	private function getAllSlaveInstance() {
		return $this->getAllInstance ( 'slave' );
	}
	private function getAllInstance($type = null) {
		foreach ( $this->hostList as $info ) {
			if ($type == null || $type == 'slave') {
				if (! isset ( $info ['slave'] ) || ! $info ['slave']) {
					continue;
				}
				foreach ( $info as $slave ) {
					$this->getInstance ( $slave );
				}
			}
			if ($type == null || $type == 'master') {
				if (! isset ( $info ['master'] ) || ! $info ['master']) {
					continue;
				}
				$this->getInstance ( $info ['master'] );
			}
		}
		return $this->instanceList;
	}
	private function getInstance($hostInfo) {
		if ((! isset ( $this->instanceList [$hostInfo] ) || $this->instanceList [$hostInfo] == NULL)) {
			$o = new \Redis ();
			$info = explode ( ':', $hostInfo );
			$o->connect ( $info [0], $info [1] );
			$this->instanceList [$hostInfo] = $o;
		}
		return $this->instanceList [$hostInfo];
	}
	
	// brpop和blpop会监听多个key，而这多个key可能会分布在多个实例上
	// 而redis:brpop只能阻塞在一个实例上，因此，需要一个全局的queue来处理当多个key都有数据这种情况
	// 调用lpush之类的操作时，首先会把数据压入key中，然后把key加入queue中
	private function push($key) {
		$this->getRedis ( $this->queueName )->lPush ( $this->queueName, $key );
		return $this->expire ( $this->queueName, 3600 );
	}
	private function pop() {
		return $this->getRedis ( $this->queueName )->rPop ( $this->queueName );
	}
	private function bpop($timeout) {
		return $this->getRedis ( $this->queueName )->brPop ( $this->queueName, $timeout );
	}
	private function getHashKey($key) {
		return isset ( $this->realkeyHashkeyMap [$key] ) ? $this->realkeyHashkeyMap [$key] : $key;
	}
}

/**
 * A simple consistent hashing implementation with pluggable hash algorithms.
 *
 * @author Paul Annesley
 * @package Flexihash
 *          @licence http://www.opensource.org/licenses/mit-license.php
 */
class Flexihash {
	
	/**
	 * The number of positions to hash each target to.
	 *
	 * @var int @comment 虚拟节点数,解决节点分布不均的问题
	 */
	private $_replicas = 64;
	
	/**
	 * The hash algorithm, encapsulated in a Flexihash_Hasher implementation.
	 *
	 * @var object Flexihash_Hasher
	 *      @comment 使用的hash方法 : md5,crc32
	 */
	private $_hasher;
	
	/**
	 * Internal counter for current number of targets.
	 *
	 * @var int @comment 节点记数器
	 */
	private $_targetCount = 0;
	
	/**
	 * Internal map of positions (hash outputs) to targets
	 *
	 * @var array { position => target, ... }
	 *      @comment 位置对应节点,用于lookup中根据位置确定要访问的节点
	 */
	private $_positionToTarget = array ();
	
	/**
	 * Internal map of targets to lists of positions that target is hashed to.
	 *
	 * @var array { target => [ position, position, ... ], ... }
	 *      @comment 节点对应位置,用于删除节点
	 */
	private $_targetToPositions = array ();
	
	/**
	 * Whether the internal map of positions to targets is already sorted.
	 *
	 * @var boolean @comment 是否已排序
	 */
	private $_positionToTargetSorted = false;
	
	/**
	 * Constructor
	 *
	 * @param object $hasher
	 *        	Flexihash_Hasher
	 * @param int $replicas
	 *        	Amount of positions to hash each target to.
	 *        	@comment 构造函数,确定要使用的hash方法和需拟节点数,虚拟节点数越多,分布越均匀,但程序的分布式运算越慢
	 */
	public function __construct(Flexihash_Hasher $hasher = null, $replicas = null) {
		$this->_hasher = $hasher ? $hasher : new Flexihash_Crc32Hasher ();
		if (! empty ( $replicas ))
			$this->_replicas = $replicas;
	}
	
	/**
	 * Add a target.
	 *
	 * @param string $target
	 *        	@chainable
	 *        	@comment 添加节点,根据虚拟节点数,将节点分布到多个虚拟位置上
	 */
	public function addTarget($target) {
		if (isset ( $this->_targetToPositions [$target] )) {
			throw new Flexihash_Exception ( "Target '$target' already exists." );
		}
		
		$this->_targetToPositions [$target] = array ();
		
		// hash the target into multiple positions
		for($i = 0; $i < $this->_replicas; $i ++) {
			$position = $this->_hasher->hash ( $target . $i );
			$this->_positionToTarget [$position] = $target; // lookup
			$this->_targetToPositions [$target] [] = $position; // target removal
		}
		
		$this->_positionToTargetSorted = false;
		$this->_targetCount ++;
		
		return $this;
	}
	
	/**
	 * Add a list of targets.
	 *
	 * @param array $targets
	 *        	@chainable
	 */
	public function addTargets(array $targets) {
		foreach ( $targets as $target ) {
			$this->addTarget ( $target );
		}
		
		return $this;
	}
	
	/**
	 * Remove a target.
	 *
	 * @param string $target
	 *        	@chainable
	 */
	public function removeTarget($target) {
		if (! isset ( $this->_targetToPositions [$target] )) {
			throw new Flexihash_Exception ( "Target '$target' does not exist." );
		}
		
		foreach ( $this->_targetToPositions [$target] as $position ) {
			unset ( $this->_positionToTarget [$position] );
		}
		
		unset ( $this->_targetToPositions [$target] );
		
		$this->_targetCount --;
		
		return $this;
	}
	
	/**
	 * A list of all potential targets
	 *
	 * @return array
	 */
	public function getAllTargets() {
		return array_keys ( $this->_targetToPositions );
	}
	
	/**
	 * Looks up the target for the given resource.
	 *
	 * @param string $resource        	
	 * @return string
	 */
	public function lookup($resource) {
		$targets = $this->lookupList ( $resource, 1 );
		if (empty ( $targets ))
			throw new Flexihash_Exception ( 'No targets exist' );
		return $targets [0];
	}
	public function lookupList($resource, $requestedCount) {
		if (! $requestedCount)
			throw new Flexihash_Exception ( 'Invalid count requested' );
			
			// handle no targets
		if (empty ( $this->_positionToTarget ))
			return array ();
			
			// optimize single target
		if ($this->_targetCount == 1)
			return array_unique ( array_values ( $this->_positionToTarget ) );
			
			// hash resource to a position
		$resourcePosition = $this->_hasher->hash ( $resource );
		
		$results = array ();
		$collect = false;
		
		$this->_sortPositionTargets ();
		
		// search values above the resourcePosition
		foreach ( $this->_positionToTarget as $key => $value ) {
			// start collecting targets after passing resource position
			if (! $collect && $key > $resourcePosition) {
				$collect = true;
			}
			
			// only collect the first instance of any target
			if ($collect && ! in_array ( $value, $results )) {
				$results [] = $value;
			}
			
			// return when enough results, or list exhausted
			if (count ( $results ) == $requestedCount || count ( $results ) == $this->_targetCount) {
				return $results;
			}
		}
		
		// loop to start - search values below the resourcePosition
		foreach ( $this->_positionToTarget as $key => $value ) {
			if (! in_array ( $value, $results )) {
				$results [] = $value;
			}
			
			// return when enough results, or list exhausted
			if (count ( $results ) == $requestedCount || count ( $results ) == $this->_targetCount) {
				return $results;
			}
		}
		
		// return results after iterating through both "parts"
		return $results;
	}
	public function __toString() {
		return sprintf ( '%s{targets:[%s]}', get_class ( $this ), implode ( ',', $this->getAllTargets () ) );
	}
	private function _sortPositionTargets() {
		// sort by key (position) if not already
		if (! $this->_positionToTargetSorted) {
			ksort ( $this->_positionToTarget, SORT_REGULAR );
			$this->_positionToTargetSorted = true;
		}
	}
}
interface Flexihash_Hasher {
	
	/**
	 * Hashes the given string into a 32bit address space.
	 *
	 * Note that the output may be more than 32bits of raw data, for example
	 * hexidecimal characters representing a 32bit value.
	 *
	 * The data must have 0xFFFFFFFF possible values, and be sortable by
	 * PHP sort functions using SORT_REGULAR.
	 *
	 * @param
	 *        	string
	 * @return mixed A sortable format with 0xFFFFFFFF possible values
	 */
	public function hash($string);
}
class Flexihash_Crc32Hasher implements Flexihash_Hasher {
	public function hash($string) {
		return crc32 ( $string );
	}
}
class Flexihash_Md5Hasher implements Flexihash_Hasher {
	public function hash($string) {
		return substr ( md5 ( $string ), 0, 8 ); // 8 hexits = 32bit
	}
}
class Flexihash_Exception extends \Exception {
}
?>