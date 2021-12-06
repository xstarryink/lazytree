/*
2020-12-30 wangxin
nosql的key-value数据库
*/
#ifndef __lazydb_h__
#define __lazydb_h__

#include <Windows.h>
#include <vector>
#include <utility>

#include "lazy_trie_16_place.h"

//返回true则结束查询
typedef bool(*lazydb_prefix_get_callback)(const char *key, const char *value, void *userdata);

class  lazydb
{

	typedef unsigned long wxt_size;
	typedef unsigned long wxt_children;
	typedef __int64 wxt_index;
	typedef unsigned long wxt_klength;
	typedef unsigned long wxt_vlength;
	typedef unsigned long wxt_hash;

	struct ilazydb_head
	{
		char flag[16];
		wxt_size number;//节点群的数量
		wxt_size count;//存储key值的数量
		GUID guid;//数据库创建时的一个随机值
		unsigned long password;//密码的hash值（暂未加密）
	};

	struct klazydb_head
	{
		char flag[16];
	};

	struct vlazydb_head
	{
		char flag[16];
	};

	struct lazydb_node
	{
		wxt_klength klength;//key值的长度
		wxt_vlength vlength;//value值的长度，实际字符串的长度可能小于这个值，后面补了0。实际意义为该位置存储过的最长的字符串长度
		wxt_children children;//子节点块的编号，最多4294967295个子节点群，插满为4,294,967,295 * 16 = 68,719,476,736 个子节点
		wxt_hash hash;//key的哈希值
		wxt_index key;//key的序号
		wxt_index value;//value的序号
	};

	struct lazydb_children
	{
		lazydb_node nodes[16];//16 * 32
	};

	//4kb
	struct lazydb_block
	{
		lazydb_children children[8];//8*16*32=4096，每一页里存储8个节点组，128个节点
	};

	//创建索引所需的结构体和回调函数
	struct memory_indexes
	{
		struct place
		{
			wxt_klength height;
			wxt_children index;
		};
		bool close;
		bool done;
		lazy_trie_16_place indexes;
	};

	typedef void(*lazydb_indexes_step_callback)(const char *key, const memory_indexes::place &place, void *userdata);

public:

	lazydb();

	~lazydb();

	unsigned long error();

	void password(const char *password);

	bool open(const char *filepath);

	bool close();

	bool reopen();

	void getpath(char *path, unsigned long size);

	bool set(const char *key, const char *value);

	void get(const char *key, char **value);

	long prefix_get(const char *key, lazydb_prefix_get_callback callback, void *userdata);

	bool del(const char *key, wxt_klength key_length);

	bool size(unsigned long &number, unsigned long &count);

	bool trim();

#ifndef lazydb_disable_memory_indexes
	void create_indexes();
#endif

private:

	//数据库相关文件的打开和关闭
	bool lazydb_open(const char *filepath);

	bool lazydb_close();

	//数据库检索文件的打开和关闭
	bool ilazydb_open_safe(const char *filepath);

	bool ilazydb_close();

	//数据库键文件的打开和关闭
	bool klazydb_open(const char *filepath);

	bool klazydb_close();

	//数据库值文件的打开和关闭
	bool vlazydb_open(const char *filepath);

	bool vlazydb_close();

	//数据库索引文件头的写入和读取
	bool ihead_write(const ilazydb_head &head);

	bool ihead_read(ilazydb_head *head);

	//数据库键文件头的写入和读取
	bool khead_write_safe(const klazydb_head &head);

	bool khead_read_safe(klazydb_head *head);

	//数据库值文件头的写入和读取
	bool vhead_write_safe(const vlazydb_head &head);

	bool vhead_read_safe(vlazydb_head *head);

	//子节点群的写入和读取
	bool children_write(__int64 children_offset, lazydb_children *children);

	bool children_read(__int64 children_offset, lazydb_children **children, DWORD &dwSize);

	//子节点的写入和读取
	bool node_write(__int64 node_offset, lazydb_node *node);

	bool node_read(__int64 node_offset, lazydb_node **node, DWORD &dwSize);

	//向文件的末尾添加数据
	bool append_write_safe(HANDLE hFile, const char *buffer, DWORD dwSize, wxt_index &offset);

	//从文件指定位置写入或读取指定大小的数据
	bool offset_write_safe(HANDLE hFile, wxt_index offset, const char *buffer, DWORD dwSize);

	bool offset_read(HANDLE hFile, wxt_index offset, char *buffer, DWORD dwSize);

	bool offset_read_safe(HANDLE hFile, wxt_index offset, char *buffer, DWORD dwSize);

	//找到key所在的reallength和children_index（occupy_to的前两个参数），若找不到则返回最后的叶子节点
	//klength_real和index传入查找的初始位置，不知道起始位置则均设为=1即可
	void find(const char *key, wxt_klength &klength_real, wxt_children &index);

	//节点驱赶
	//head：头信息；length_real：驱赶到哪一层；index：驱赶到那个块上；key：键；value：值；number：当前的子节点群个数
	bool occupy_to(wxt_klength length_real, wxt_children index, const char *key, const char *value);

	//获取前缀的所有后代
	long prefix_all_descendant(lazydb_children *children, lazydb_prefix_get_callback callback, void *userdata, bool &bbreak);

#ifndef lazydb_disable_memory_indexes
	//创建索引的单步
	void create_indexes_step(wxt_klength length_real, wxt_children index, lazydb_indexes_step_callback callback, void *userdata);

	//更新索引
	void update_indexex(const char *key, const memory_indexes::place &place);
#endif

	//计算字符串的hash
	unsigned long bkdr_hash(const char *str);

private:

	HANDLE m_hIlazydb;

	HANDLE m_hKlazydb;

	HANDLE m_hVlazydb;

	char m_sFilePath[MAX_PATH];

	GUID m_guid;

	unsigned long m_ulHashPassword;

	unsigned long m_ulError;

#ifndef lazydb_disable_memory_indexes
	memory_indexes *m_pIndexes;

	struct IndexexList
	{
		int index;
		std::vector<std::pair<std::string, memory_indexes::place>> array[2];
	};
	IndexexList *m_pIndexexList;
#endif

};

#endif //__lazydb_h__
