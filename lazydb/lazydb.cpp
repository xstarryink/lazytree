#include "stdafx.h"

#pragma comment(lib, "Shlwapi.lib")
#include <Shlwapi.h>
#include <io.h>

#include "lazydb.h"
#include "lazydb_define.h"

#ifndef lazydb_disable_memory_indexes
#include <thread>
#endif
#ifdef lazydb_one_process
#define wx_lock()
#define do_wx_lock()
#define wx_unlock()
#else
#include "process_lock.h"
#define wx_lock(l,f,t,o,s) \
file_lock l;\
do\
{\
l.lock(f,t,o,s);\
}\
while(0)
#define do_wx_lock(l,f,t,o,s) \
do\
{\
l.lock(f,t,o,s);\
}\
while(0)
#define wx_unlock(l) l.unlock()
#endif

#define DISK_BLOCK_SIZE 4096 //认为磁盘每一块的大小为4k
#ifndef ILAZYDB_HEAD_FLAG
#define ILAZYDB_HEAD_FLAG "ilazydb" //文件头的标识
#endif
#ifndef KLAZYDB_HEAD_FLAG
#define KLAZYDB_HEAD_FLAG "klazydb" //文件头的标识
#endif
#ifndef VLAZYDB_HEAD_FLAG
#define VLAZYDB_HEAD_FLAG "vlazydb" //文件头的标识
#endif

lazydb::lazydb()
	:m_hIlazydb(NULL)
	, m_hKlazydb(NULL)
	, m_hVlazydb(NULL)
	, m_guid({ 0 })
	, m_ulHashPassword(0)
	, m_ulError(0)
#ifndef lazydb_disable_memory_indexes
	, m_pIndexes(nullptr)
	, m_pIndexexList(nullptr)
#endif
{
	memset(m_sFilePath, 0, MAX_PATH);
}

lazydb::~lazydb()
{}

unsigned long lazydb::error()
{
	return m_ulError;
}

void lazydb::password(const char *password)
{
	m_ulHashPassword = bkdr_hash(password);
}

bool lazydb::open(const char *filepath)
{
	lazydb_close();
	strcpy_s(m_sFilePath, filepath);
	return lazydb_open(filepath);
}

bool lazydb::close()
{
#ifndef lazydb_disable_memory_indexes
	if (m_pIndexes)
	{
		m_pIndexes->close = true;
	}
#endif
	return lazydb_close();
}

bool lazydb::reopen()
{
	lazydb_close();
	//或许可以用::ReOpenFile()来重新打开
	return lazydb_open(m_sFilePath);
}

void lazydb::getpath(char *path, unsigned long size)
{
	if (m_sFilePath[1] != ':')
	{
		::GetCurrentDirectoryA(MAX_PATH, path);
		::PathAppendA(path, m_sFilePath);
	}
	else
	{
		memcpy(path, m_sFilePath, min(size, MAX_PATH));
	}
}

bool lazydb::set(const char *key, const char *value)
{
	ilazydb_head head = { 0 };
	wx_lock(pl_read, m_hIlazydb, file_lock::type::read, 0, sizeof(ilazydb_head));
	if (ihead_read(&head))
	{
		wx_unlock(pl_read);
		if (m_guid == head.guid)
		{
			wxt_klength length_real = 1;
			wxt_children index = 1;
			find(key, length_real, index);

            //锁相关测试：wx_lock(pl_iwrite, m_hIlazydb, file_lock::type::write, 2ULL * 1024ULL * 1024ULL * 1024ULL, 1);
			return occupy_to(length_real, index, key, value);
		}
		else
		{
			reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
			return false;
		}
	}
	else
	{
		wx_unlock(pl_read);
		return false;
	}
}

void lazydb::get(const char *key, char **value)
{
	//检查是否需要重新打开文件
	ilazydb_head head = { 0 };
	//注意，这里不加锁打开也行，因为不需要确切知道guid的值，只需要知道是否有变化即可，写了一半的也没关系。reopen里获取值的时候会加锁
	//wx_lock(pl_read, m_hIlazydb, file_lock::type::read, 0, sizeof(ilazydb_head));
	if (ihead_read(&head))
	{
		//wx_unlock(pl_read);
		if (m_guid != head.guid)
		{
			reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
		}
	}
	//else
	//{
	//	wx_unlock(pl_read);
	//}

	wxt_children index = 1;
	wxt_klength klength_real = 1;

#ifndef lazydb_disable_memory_indexes
	//先查索引
	memory_indexes::place place = { 0, 0 };
	if (m_pIndexes && m_pIndexes->done)
	{
		wxt_klength height_tmp = 0;
		wxt_children index_tmp = 0;
		if (m_pIndexes->indexes.try_get(key, height_tmp, index_tmp))
		{
			place = { height_tmp, index_tmp };
			klength_real = height_tmp;
			index = index_tmp;
		}
	}
#endif

	//开始搜索
	lazydb_node *node = nullptr;
	wxt_hash key_hash = (wxt_hash)bkdr_hash(key);
	wxt_klength key_length = (wxt_klength)strlen(key),
		key_length_real = key_length << 1;
	for (wxt_klength i = klength_real - 1; i < key_length_real; ++i)
	{
		if (index == 0) break;//不存在子节点，说明到了叶子节点

		unsigned long key_real = 0xfUL & (i & 1UL ? key[i >> 1] : (key[i >> 1] >> 4));
		//sizeof(head) + (index - 1) * sizeof(lazydb_children) + realkey * sizeof(lazydb_node),index是从1开始的
		__int64	node_offset = (__int64)DISK_BLOCK_SIZE + ((index - 1) << 9) + (key_real << 5);

		DWORD dwReadSize = 0;
		wx_lock(pl_iread, m_hIlazydb, file_lock::type::read, node_offset, sizeof(lazydb_node));
		if (node_read(node_offset, &node, dwReadSize))
		{
			wx_unlock(pl_iread);
			if (node->klength == 0)
			{
				break;
			}
			else if (klength_real == key_length_real)
			{
				//因为已经走到头了，key的每一个字符都已经遍历过，那么如果两个值长度相等，
				//便意味着每个字符都相等，即字符串相等：长度相等 <=> 字符串相等
				//反之（逆否） ： 长度不相等 <=> 字符串不相等
				if (node->klength == key_length)
				{
					if (*value) delete[] *value;
					*value = new char[node->vlength + 1]();
					if (offset_read_safe(m_hVlazydb, node->value, *value, node->vlength))
					{
					}
#ifndef lazydb_disable_memory_indexes
					if (m_pIndexes && m_pIndexes->done && (place.height != klength_real || place.index != index))
					{
						update_indexex(key, { klength_real, index });
					}
#endif
				}
				break;
			}
			else
			{
				//先判断长度和hash是否相等
				if (node->klength == key_length && node->hash == key_hash)
				{
					bool exists = false;
					char *kbuffer = new char[node->klength + 1]();
					if (offset_read_safe(m_hKlazydb, node->key, kbuffer, node->klength))
					{
						if (strcmp(key, kbuffer) == 0)
						{
							if (*value) delete[] *value;
							*value = new char[node->vlength + 1]();
							if (offset_read_safe(m_hVlazydb, node->value, *value, node->vlength))
							{
								exists = true;
							}
#ifndef lazydb_disable_memory_indexes
							if (m_pIndexes && m_pIndexes->done && (place.height != klength_real || place.index != index))
							{
								update_indexex(key, { klength_real, index });
							}
#endif
						}
					}
					delete[] kbuffer;
					if (exists)
					{
						break;
					}
				}
				index = node->children;
			}
		}
		else
		{
			wx_unlock(pl_iread);
			break;
		}
		++klength_real;
	}
	if (node)
	{
		delete node;
	}
}

long lazydb::prefix_get(const char *key, lazydb_prefix_get_callback callback, void *userdata)
{
	int count = 0;
	//检查是否需要重新打开文件
	ilazydb_head head = { 0 };
	//注意，这里不加锁打开也行，因为不需要确切知道guid的值，只需要知道是否有变化即可，写了一半的也没关系。reopen里获取值的时候会加锁
	//wx_lock(pl_read, m_hIlazydb, file_lock::type::read, 0, sizeof(ilazydb_head));
	if (ihead_read(&head))
	{
		//wx_unlock(pl_read);
		if (m_guid != head.guid)
		{
			reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
		}
	}
	//else
	//{
	//	wx_unlock(pl_read);
	//}
	//开始搜索
	lazydb_node *node = nullptr;
	wxt_children index = 1;
	wxt_klength key_length = (wxt_klength)strlen(key);
	bool bbreak = false;
	for (wxt_klength i = 0; ; ++i)
	{
		if (index == 0) break;//不存在子节点，说明到了叶子节点

		unsigned long key_real = 0xfUL & (i & 1UL ? key[i >> 1] : (key[i >> 1] >> 4));
		//sizeof(head) + (index - 1) * sizeof(lazydb_children) + realkey * sizeof(lazydb_node),index是从1开始的
		__int64	node_offset = (__int64)DISK_BLOCK_SIZE + ((index - 1) << 9) + (key_real << 5);

		DWORD dwReadSize = 0;
		wx_lock(pl_iread, m_hIlazydb, file_lock::type::read, node_offset, sizeof(lazydb_node));
		if (node_read(node_offset, &node, dwReadSize))
		{
			wx_unlock(pl_iread);
			if ((i >> 1) < key_length)
			{
				if (node->klength == 0)
				{
					break;
				}
				else
				{
					if (node->klength >= key_length)
					{
						char *kbuffer = new char[node->klength + 1]();
						if (offset_read_safe(m_hKlazydb, node->key, kbuffer, node->klength))
						{
							bool match = true;
							for (int char_index = i >> 1; char_index < key_length; ++char_index)
							{
								if (kbuffer[char_index] != key[char_index])
								{
									match = false;
									break;
								}
							}
							if (match)
							{
								char *vbuffer = new char[node->vlength + 1]();
								if (offset_read_safe(m_hVlazydb, node->value, vbuffer, node->vlength))
								{
									bbreak = callback(kbuffer, vbuffer, userdata);
									++count;
								}
								delete[] vbuffer;
							}
						}
						delete[] kbuffer;
					}
					index = node->children;
				}
			}
			else
			{
				lazydb_children *children = nullptr;
				__int64	children_offset = (__int64)DISK_BLOCK_SIZE + ((index - 1) << 9);
				DWORD dwReadSize = 0;
				wx_lock(pl_iread, m_hIlazydb, file_lock::type::read, children_offset, sizeof(lazydb_children));
				if (children_read(children_offset, &children, dwReadSize))
				{
					wx_unlock(pl_iread);
					count += prefix_all_descendant(children, callback, userdata, bbreak);
				}
				else
				{
					wx_unlock(pl_iread);
				}
				if (children)
				{
					delete children;
				}
				break;
			}
		}
		else
		{
			wx_unlock(pl_iread);
			break;
		}
		if (bbreak) break;
	}
	if (node)
	{
		delete node;
	}
	return count;
}

bool lazydb::del(const char *key, wxt_klength key_length)
{
	//需要注意，del之后节点数量不会减少，甚至可能增加
	return set(key, "");
}

bool lazydb::size(unsigned long &number, unsigned long &count)
{
	ilazydb_head head = { 0 };
	wx_lock(pl_read, m_hIlazydb, file_lock::type::read, 0, sizeof(ilazydb_head));
	if (ihead_read(&head))
	{
		wx_unlock(pl_read);
		if (m_guid == head.guid)
		{
			number = head.number;
			count = head.count;
			return true;
		}
		else
		{
			reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
			return false;
		}
	}
	else
	{
		wx_unlock(pl_read);
		return false;
	}
}

#ifndef lazydb_disable_memory_indexes
void lazydb::create_indexes()
{
	if (m_pIndexes == nullptr)
	{
		m_pIndexes = new memory_indexes({ 0 });
		m_pIndexes->close = false;
		m_pIndexes->done = false;
		memory_indexes *pIndexes = m_pIndexes;

		m_pIndexexList = new IndexexList();
		m_pIndexexList->index = 0;
		m_pIndexexList->array[0].clear();
		m_pIndexexList->array[1].clear();
		IndexexList *pIndexexList = m_pIndexexList;

		pIndexes->done = false;
		create_indexes_step(1, 1, [](const char *key, const memory_indexes::place &place, void *userdata)
		{
			if (key  && userdata)
			{
				memory_indexes *pMemoryIndexes = (memory_indexes*)userdata;
				pMemoryIndexes->indexes.set(key, place.height, place.index);
			}
		}, pIndexes);
		pIndexes->done = true;

		std::thread thrCreateIndexes([this, pIndexes, pIndexexList]()
		{
			while (!pIndexes->close)
			{
				Sleep(1000);
				//两个容器切换使用，避免加锁
				int iQueueIndexNext = (pIndexexList->index + 1) & 1;
				std::vector<std::pair<std::string, memory_indexes::place>> &vctIndexes = pIndexexList->array[iQueueIndexNext];
				for (const auto &iter : vctIndexes)
				{
					pIndexes->indexes.set(iter.first, iter.second.height, iter.second.index);
				}
				vctIndexes.clear();
				pIndexexList->index = iQueueIndexNext;
			}
			delete pIndexes;
			delete pIndexexList;
		});
		thrCreateIndexes.detach();
	}
}
#endif

bool lazydb::trim()
{
	//对3个文件进行整理，释放无用的空间
	//整理时会锁住整个表，不能写入只能读取
	ilazydb_head head = { 0 };
	unsigned long ulFileSizeHeight = 0,
		ulFileSizeLow = ::GetFileSize(m_hIlazydb, &ulFileSizeHeight);
	LARGE_INTEGER liFileSize;
	liFileSize.LowPart = ulFileSizeLow;
	liFileSize.HighPart = ulFileSizeHeight;
	if (liFileSize.QuadPart  > 0)
	{
		wx_lock(pl_read, m_hIlazydb, file_lock::type::read, 0, liFileSize.QuadPart);
		//一次性读到内存里来速度会更快，但可能会需要过多的内存，有利有弊。
		if (ihead_read(&head))
		{
			if (m_guid != head.guid)
			{
				wx_unlock(pl_read);
				reopen();//只可能是另外一个程序trim了数据库，需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
				return false;
			}

			char sTrim[MAX_PATH] = { 0 };
			strcpy_s(sTrim, m_sFilePath);
			strcat_s(sTrim, "~trim");

			lazydb dbTrim;
            dbTrim.open(sTrim);
            dbTrim.m_ulHashPassword = m_ulHashPassword;

			lazydb_children *children = nullptr;
			for (int i = 1; i <= head.number; ++i)
			{
				DWORD dwReadSize = 0;
				__int64	children_offset = (__int64)DISK_BLOCK_SIZE + ((i - 1) << 9);//index * sizeof(lazydb_children),index是从1开始的
				if (children_read(children_offset, &children, dwReadSize))
				{
					for (unsigned long n = 0; n < 16; ++n)
					{
						const lazydb_node &node = children->nodes[n];
						if (node.klength != 0)
						{
							char *skey = nullptr;
							if (node.klength)
							{
								skey = new char[node.klength + 1]();
								if (!offset_read(m_hKlazydb, node.key, skey, node.klength))
								{
									delete[] skey;
									continue;
								}
							}
							char *svalue = nullptr;
							if (node.vlength)
							{
								svalue = new char[node.vlength + 1]();
								if (!offset_read(m_hVlazydb, node.value, svalue, node.vlength))
								{
									delete[] svalue;
									continue;
								}
							}
							if (skey && svalue && svalue[0])
							{
                                dbTrim.set(skey, svalue);
							}
							if (skey) delete[] skey;
							if (svalue) delete[] svalue;
						}
					}
				}
			}

            head.guid = dbTrim.m_guid;//重新写一下头，不然readfile的时候，读出来的数据不会变
            dbTrim.close();
			wx_unlock(pl_read);
		}
		else
		{
			wx_unlock(pl_read);
			return false;
		}

		wx_lock(pl_iwrite, m_hIlazydb, file_lock::type::write, 0, liFileSize.QuadPart);
		char sIlazydbSrc[MAX_PATH] = { 0 },
			sKlazydbSrc[MAX_PATH] = { 0 },
			sVlazydbSrc[MAX_PATH] = { 0 },
			sIlazydbDst[MAX_PATH] = { 0 },
			sKlazydbDst[MAX_PATH] = { 0 },
			sVlazydbDst[MAX_PATH] = { 0 };
		strcpy_s(sIlazydbSrc, m_sFilePath);
		strcat_s(sIlazydbSrc, "~trim.lazydb");
		strcpy_s(sKlazydbSrc, m_sFilePath);
		strcat_s(sKlazydbSrc, "~trim.klazydb");
		strcpy_s(sVlazydbSrc, m_sFilePath);
		strcat_s(sVlazydbSrc, "~trim.vlazydb");
		strcpy_s(sIlazydbDst, m_sFilePath);
		strcat_s(sIlazydbDst, ".lazydb");
		strcpy_s(sKlazydbDst, m_sFilePath);
		strcat_s(sKlazydbDst, ".klazydb");
		strcpy_s(sVlazydbDst, m_sFilePath);
		strcat_s(sVlazydbDst, ".vlazydb");
		::DeleteFileA(sIlazydbDst);
		::DeleteFileA(sKlazydbDst);
		::DeleteFileA(sVlazydbDst);
		rename(sIlazydbSrc, sIlazydbDst);
		rename(sKlazydbSrc, sKlazydbDst);
		rename(sVlazydbSrc, sVlazydbDst);
		//重新写一下头，不然readfile的时候，读出来的数据不会变
		if (!ihead_write(head))
		{
			//出现这样的情况，得删除，这样比数据写乱了要好一些
			::DeleteFileA(sIlazydbDst);
			::DeleteFileA(sKlazydbDst);
			::DeleteFileA(sVlazydbDst);
		}
		wx_unlock(pl_iwrite);
		return true;
	}
	return false;
}

//数据库相关文件的打开和关闭
bool lazydb::lazydb_open(const char *filepath)
{
	char sIlazydb[MAX_PATH] = { 0 },
		sKlazydb[MAX_PATH] = { 0 },
		sVlazydb[MAX_PATH] = { 0 },
		sIlazydbTrim[MAX_PATH] = { 0 },
		sKlazydbTrim[MAX_PATH] = { 0 },
		sVlazydbTrim[MAX_PATH] = { 0 };
	strcpy_s(sIlazydb, filepath);
	strcat_s(sIlazydb, ".lazydb");
	strcpy_s(sKlazydb, filepath);
	strcat_s(sKlazydb, ".vlazydb");
	strcpy_s(sVlazydb, filepath);
	strcat_s(sVlazydb, ".klazydb");
	strcpy_s(sIlazydbTrim, filepath);
	strcat_s(sIlazydbTrim, "~trim.lazydb");
	strcpy_s(sKlazydbTrim, filepath);
	strcat_s(sKlazydbTrim, "~trim.klazydb");
	strcpy_s(sVlazydbTrim, filepath);
	strcat_s(sVlazydbTrim, "~trim.vlazydb");
	if (::PathFileExistsA(sIlazydbTrim))
	{
		if (!::PathFileExistsA(sIlazydb))
		{
			::DeleteFileA(sIlazydb);
			::DeleteFileA(sKlazydb);
			::DeleteFileA(sVlazydb);
			rename(sIlazydbTrim, sIlazydb);
			rename(sKlazydbTrim, sKlazydb);
			rename(sVlazydbTrim, sVlazydb);
		}
		else
		{
			::DeleteFileA(sIlazydbTrim);
			::DeleteFileA(sKlazydbTrim);
			::DeleteFileA(sVlazydbTrim);
		}
	}
	return ilazydb_open_safe(sIlazydb)
		&& klazydb_open(sKlazydb)
		&& vlazydb_open(sVlazydb);
}

bool lazydb::lazydb_close()
{
	return ilazydb_close()
		&& klazydb_close()
		&& vlazydb_close();
}

//数据库检索文件的打开和关闭
bool lazydb::ilazydb_open_safe(const char *filepath)
{
	m_hIlazydb = ::CreateFileA(
		filepath,
		GENERIC_WRITE | GENERIC_READ,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		NULL);
	if (m_hIlazydb == INVALID_HANDLE_VALUE)
	{
		SECURITY_ATTRIBUTES sa;
		SECURITY_DESCRIPTOR sd;
		InitializeSecurityDescriptor(&sd, SECURITY_DESCRIPTOR_REVISION);
		SetSecurityDescriptorDacl(&sd, TRUE, NULL, FALSE);
		sa.nLength = sizeof(SECURITY_ATTRIBUTES);
		sa.lpSecurityDescriptor = &sd;

		m_hIlazydb = ::CreateFileA(
			filepath,
			GENERIC_WRITE | GENERIC_READ,
			FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
			&sa,
			OPEN_ALWAYS,
			FILE_ATTRIBUTE_NORMAL,
			NULL);
	}
	if (m_hIlazydb == INVALID_HANDLE_VALUE)
	{
		m_hIlazydb = NULL;
		m_ulError = LAZYDB_ERROR_FILE;
		return false;
	}
	if (::GetFileSize(m_hIlazydb, NULL) == 0)
	{
		wx_lock(pl_wirte, m_hIlazydb, file_lock::type::write, 0, sizeof(ilazydb_head));
		unsigned char init_data[DISK_BLOCK_SIZE] = { 0 };
		DWORD dwSize = 0;
		if (::WriteFile(m_hIlazydb, init_data, DISK_BLOCK_SIZE, &dwSize, NULL))
		{
			wx_unlock(pl_wirte);
			if (DISK_BLOCK_SIZE == dwSize)
			{
				ilazydb_head head = { 0 };
				strcpy_s(head.flag, ILAZYDB_HEAD_FLAG);
				head.number = 0;
				head.count = 0;
				CoCreateGuid(&head.guid);
				head.password = m_ulHashPassword;
				m_guid = head.guid;
				if(ihead_write(head))
				{
					m_ulError = LAZYDB_SUCCESS;
					return true;
				}
				else
				{
					m_ulError = LAZYDB_ERROR_FILE;
				}
			}
			else
			{
				m_ulError = LAZYDB_ERROR_FILE;
			}
		}
		else
		{
			wx_unlock(pl_wirte);
			m_ulError = LAZYDB_ERROR_FILE;
		}
	}
	else
	{
		ilazydb_head head = { 0 };
		if (ihead_read(&head))
		{
			if (head.password == m_ulHashPassword)
			{
				m_guid = head.guid;
				if (strcmp(head.flag, ILAZYDB_HEAD_FLAG) == 0)
				{
					m_ulError = LAZYDB_SUCCESS;
					return true;
				}
				else
				{
					m_ulError = LAZYDB_ERROR_VERSION;
				}
			}
			else
			{
				m_ulError = LAZYDB_ERROR_PASSWORD;
			}
		}
		else
		{
			m_ulError = LAZYDB_ERROR_FILE;
		}
	}
	if (m_ulError == LAZYDB_SUCCESS) m_ulError = LAZYDB_ERROR_FILE;
	return false;
}

bool lazydb::ilazydb_close()
{
	if (::CloseHandle(m_hIlazydb))
	{
		m_hIlazydb = NULL;
		return true;
	}
	return false;
}

//数据库键文件的打开和关闭
bool lazydb::klazydb_open(const char *filepath)
{
	m_hKlazydb = ::CreateFileA(
		filepath,
		GENERIC_WRITE | GENERIC_READ,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		NULL);
	if (m_hKlazydb == INVALID_HANDLE_VALUE)
	{
		SECURITY_ATTRIBUTES sa;
		SECURITY_DESCRIPTOR sd;
		InitializeSecurityDescriptor(&sd, SECURITY_DESCRIPTOR_REVISION);
		SetSecurityDescriptorDacl(&sd, TRUE, NULL, FALSE);
		sa.nLength = sizeof(SECURITY_ATTRIBUTES);
		sa.lpSecurityDescriptor = &sd;

		m_hKlazydb = ::CreateFileA(
			filepath,
			GENERIC_WRITE | GENERIC_READ,
			FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
			&sa,
			OPEN_ALWAYS,
			FILE_ATTRIBUTE_NORMAL,
			NULL);
	}
	if (m_hKlazydb == INVALID_HANDLE_VALUE)
	{
		m_hKlazydb = NULL;
		m_ulError = LAZYDB_ERROR_FILE;
		return false;
	}
	if (::GetFileSize(m_hKlazydb, NULL) == 0)
	{
		klazydb_head head = { 0 };
		strcpy_s(head.flag, KLAZYDB_HEAD_FLAG);
		if (khead_write_safe(head))
		{
			m_ulError = LAZYDB_SUCCESS;
			return true;
		}
		else
		{
			m_ulError = LAZYDB_ERROR_FILE;
		}
	}
	else
	{
		klazydb_head head = { 0 };
		if (khead_read_safe(&head))
		{
			if(strcmp(head.flag, KLAZYDB_HEAD_FLAG) == 0)
			{
				m_ulError = LAZYDB_SUCCESS;
				return true;
			}
			else
			{
				m_ulError = LAZYDB_ERROR_VERSION;
			}
		}
		else
		{
			m_ulError = LAZYDB_ERROR_FILE;
		}
	}
	if (m_ulError == LAZYDB_SUCCESS) m_ulError = LAZYDB_ERROR_FILE;
	return false;
}

bool lazydb::klazydb_close()
{
	if (::CloseHandle(m_hKlazydb))
	{
		m_hKlazydb = NULL;
		return true;
	}
	return false;
}

//数据库值文件的打开和关闭
bool lazydb::vlazydb_open(const char *filepath)
{
	m_hVlazydb = ::CreateFileA(
		filepath,
		GENERIC_WRITE | GENERIC_READ,
		FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
		NULL,
		OPEN_EXISTING,
		FILE_ATTRIBUTE_NORMAL,
		NULL);
	if (m_hVlazydb == INVALID_HANDLE_VALUE)
	{
		SECURITY_ATTRIBUTES sa;
		SECURITY_DESCRIPTOR sd;
		InitializeSecurityDescriptor(&sd, SECURITY_DESCRIPTOR_REVISION);
		SetSecurityDescriptorDacl(&sd, TRUE, NULL, FALSE);
		sa.nLength = sizeof(SECURITY_ATTRIBUTES);
		sa.lpSecurityDescriptor = &sd;

		m_hVlazydb = ::CreateFileA(
			filepath,
			GENERIC_WRITE | GENERIC_READ,
			FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
			&sa,
			OPEN_ALWAYS,
			FILE_ATTRIBUTE_NORMAL,
			NULL);
	}
	if (m_hVlazydb == INVALID_HANDLE_VALUE)
	{
		m_hVlazydb = NULL;
		m_ulError = LAZYDB_ERROR_FILE;
		return false;
	}
	if (::GetFileSize(m_hVlazydb, NULL) == 0)
	{
		vlazydb_head head = { 0 };
		strcpy_s(head.flag, VLAZYDB_HEAD_FLAG);
		if (vhead_write_safe(head))
		{
			m_ulError = LAZYDB_SUCCESS;
			return true;
		}
		else
		{
			m_ulError = LAZYDB_ERROR_FILE;
		}
	}
	else
	{
		vlazydb_head head = { 0 };
		if (vhead_read_safe(&head))
		{
			if (strcmp(head.flag, VLAZYDB_HEAD_FLAG) == 0)
			{
				m_ulError = LAZYDB_SUCCESS;
				return true;
			}
			else
			{
				m_ulError = LAZYDB_ERROR_VERSION;
			}
		}
		else
		{
			m_ulError = LAZYDB_ERROR_FILE;
		}
	}
	if (m_ulError == LAZYDB_SUCCESS) m_ulError = LAZYDB_ERROR_FILE;
	return false;
}

bool lazydb::vlazydb_close()
{
	if (::CloseHandle(m_hVlazydb))
	{
		m_hVlazydb = NULL;
		return true;
	}
	return false;
}

//数据库索引文件头的写入和读取
bool lazydb::ihead_write(const ilazydb_head &head)
{
	if (::SetFilePointer(m_hIlazydb, 0, NULL, FILE_BEGIN) == 0)
	{
		DWORD dwSize = 0;
		if (::WriteFile(m_hIlazydb, &head, sizeof(ilazydb_head), &dwSize, NULL))
		{
			return true;
		}
	}
	return false;
}

bool lazydb::ihead_read(ilazydb_head *head)
{
	if (head)
	{
		if (::SetFilePointer(m_hIlazydb, 0, NULL, FILE_BEGIN) == 0)
		{
			DWORD dwSize = 0;
			if (::ReadFile(m_hIlazydb, head, sizeof(ilazydb_head), &dwSize, NULL))
			{
				return sizeof(ilazydb_head) == dwSize;
			}
		}
	}
	return false;
}

//数据库键文件头的写入和读取
bool lazydb::khead_write_safe(const klazydb_head &head)
{
	if (::SetFilePointer(m_hKlazydb, 0, NULL, FILE_BEGIN) == 0)
	{
		DWORD dwSize = 0;
		wx_lock(pl_wirte, m_hKlazydb, file_lock::type::write, 0, sizeof(klazydb_head));
		if (::WriteFile(m_hKlazydb, &head, sizeof(klazydb_head), &dwSize, NULL))
		{
			wx_unlock(pl_wirte);
			return true;
		}
		wx_unlock(pl_wirte);
	}
	return false;
}

bool lazydb::khead_read_safe(klazydb_head *head)
{
	if (head)
	{
		if (::SetFilePointer(m_hKlazydb, 0, NULL, FILE_BEGIN) == 0)
		{
			DWORD dwSize = 0;
			wx_lock(pl_read, m_hKlazydb, file_lock::type::read, 0, sizeof(klazydb_head));
			if (::ReadFile(m_hKlazydb, head, sizeof(klazydb_head), &dwSize, NULL))
			{
				wx_unlock(pl_read);
				return sizeof(klazydb_head) == dwSize;
			}
			wx_unlock(pl_read);
		}
	}
	return false;
}

//数据库值文件头的写入和读取
bool lazydb::vhead_write_safe(const vlazydb_head &head)
{
	if (::SetFilePointer(m_hVlazydb, 0, NULL, FILE_BEGIN) == 0)
	{
		DWORD dwSize = 0;
		wx_lock(pl_wirte, m_hVlazydb, file_lock::type::write, 0, sizeof(vlazydb_head));
		if (::WriteFile(m_hVlazydb, &head, sizeof(vlazydb_head), &dwSize, NULL))
		{
			wx_unlock(pl_wirte);
			return true;
		}
		wx_unlock(pl_wirte);
	}
	return false;
}

bool lazydb::vhead_read_safe(vlazydb_head *head)
{
	if (head)
	{
		if (::SetFilePointer(m_hVlazydb, 0, NULL, FILE_BEGIN) == 0)
		{
			DWORD dwSize = 0;
			wx_lock(pl_read, m_hVlazydb, file_lock::type::read, 0, sizeof(vlazydb_head));
			if (::ReadFile(m_hVlazydb, head, sizeof(vlazydb_head), &dwSize, NULL))
			{
				wx_unlock(pl_read);
				return sizeof(vlazydb_head) == dwSize;
			}
			wx_unlock(pl_read);
		}
	}
	return false;
}

//子节点群的写入和读取
bool lazydb::children_write(__int64 children_offset, lazydb_children *children)
{
	if (children_offset && children)
	{
		LARGE_INTEGER offset;
		offset.QuadPart = children_offset;
		if (::SetFilePointer(m_hIlazydb, (long)offset.LowPart, &offset.HighPart, FILE_BEGIN) == offset.LowPart)
		{
			DWORD dwSize = 0;
			if (::WriteFile(m_hIlazydb, children, sizeof(lazydb_children), &dwSize, NULL))
			{
				return sizeof(lazydb_children) == dwSize;
			}
		}
	}
	return false;
}

bool lazydb::children_read(__int64 children_offset, lazydb_children **children, DWORD &dwSize)
{
	//lazydb_children *children = &(block->children[index_remainder]);
	if (children_offset && children)
	{
		LARGE_INTEGER offset;
		offset.QuadPart = children_offset;
		if (::SetFilePointer(m_hIlazydb, (long)offset.LowPart, &offset.HighPart, FILE_BEGIN) == offset.LowPart)
		{
			if (*children == nullptr)
			{
				*children = new lazydb_children();
			}
			else
			{
				memset(*children, 0, sizeof(lazydb_children));
			}
			if (::ReadFile(m_hIlazydb, *children, sizeof(lazydb_children), &dwSize, NULL))
			{
				//读到节点或者节点未曾创建都返回true
				if (sizeof(lazydb_children) == dwSize || 0 == dwSize)
				{
					return true;
				}
			}
			delete *children;
			*children = nullptr;
		}
	}
	return false;
}

//子节点的写入和读取
bool lazydb::node_write(__int64 node_offset, lazydb_node *node)
{
	if (node_offset && node)
	{
		//如果节点已经存在，则更新节点
		LARGE_INTEGER offset;
		offset.QuadPart = node_offset;
		if (::SetFilePointer(m_hIlazydb, (long)offset.LowPart, &offset.HighPart, FILE_BEGIN) == offset.LowPart)
		{
			DWORD dwSize = 0;
			if (::WriteFile(m_hIlazydb, node, sizeof(lazydb_node), &dwSize, NULL))
			{
				return sizeof(lazydb_node) == dwSize;
			}
		}
	}
	return false;
}

bool lazydb::node_read(__int64 node_offset, lazydb_node **node, DWORD &dwSize)
{
	if (node_offset && node)
	{
		LARGE_INTEGER offset;
		offset.QuadPart = node_offset;
		if (::SetFilePointer(m_hIlazydb, (long)offset.LowPart, &offset.HighPart, FILE_BEGIN) == offset.LowPart)
		{
			if (*node == nullptr)
			{
				*node = new lazydb_node();
			}
			else
			{
				memset(*node, 0, sizeof(lazydb_node));
			}
			if (::ReadFile(m_hIlazydb, *node, sizeof(lazydb_node), &dwSize, NULL))
			{
				//读到节点或者节点未曾创建都返回true
				if (sizeof(lazydb_node) == dwSize || 0 == dwSize)
				{
					return true;
				}
			}
			delete *node;
			*node = nullptr;
		}
	}
	return false;
}

//向文件的末尾添加数据
bool lazydb::append_write_safe(HANDLE hFile, const char *buffer, DWORD dwSize, wxt_index &offset)
{
    /*
    LARGE_INTEGER li_offset;
    DWORD dwHeight = 0;
    wx_lock(pl_wirte, hFile, file_lock::type::write, 0, 1);
    li_offset.LowPart = ::GetFileSize(hFile, &dwHeight);
    li_offset.HighPart = dwHeight;
    offset = li_offset.QuadPart;
    if (offset)
    {
      bool bret = offset_write_safe(hFile, offset, buffer, dwSize);
      wx_unlock(pl_wirte);
      return bret;
    }
    wx_unlock(pl_wirte);
    return false;
    */
    LARGE_INTEGER li_offset;
    DWORD dwHeight = 0;
    wx_lock(pl_wirte, hFile, file_lock::type::write, 0, 1);
    li_offset.LowPart = ::GetFileSize(hFile, &dwHeight);
    li_offset.HighPart = dwHeight;
    offset = li_offset.QuadPart;
    if (offset)
    {
        if (::SetFilePointer(hFile, 0, NULL, FILE_END) == li_offset.LowPart)
        {
            DWORD dwWriteSize = 0;
            if (::WriteFile(hFile, buffer, dwSize, &dwWriteSize, NULL))
            {
                wx_unlock(pl_wirte);
                return dwSize == dwWriteSize;
            }
        }
    }
    wx_unlock(pl_wirte);
    return false;
}

//从文件指定位置写入或读取指定大小的数据
bool lazydb::offset_write_safe(HANDLE hFile, wxt_index offset, const char *buffer, DWORD dwSize)
{
	LARGE_INTEGER li_offset;
	li_offset.QuadPart = offset;
	if (::SetFilePointer(hFile, (long)li_offset.LowPart, &li_offset.HighPart, FILE_BEGIN) == li_offset.LowPart)
	{
		DWORD dwWriteSize = 0;
		wx_lock(pl_wirte, hFile, file_lock::type::write, offset, dwSize);
		if (::WriteFile(hFile, buffer, dwSize, &dwWriteSize, NULL))
		{
			wx_unlock(pl_wirte);
			return dwSize == dwWriteSize;
		}
		wx_unlock(pl_wirte);
	}
	return false;
}

bool lazydb::offset_read(HANDLE hFile, wxt_index offset, char *buffer, DWORD dwSize)
{
	LARGE_INTEGER li_offset;
	li_offset.QuadPart = offset;
	if (::SetFilePointer(hFile, (long)li_offset.LowPart, &li_offset.HighPart, FILE_BEGIN) == li_offset.LowPart)
	{
		DWORD dwReadSize = 0;
		if (::ReadFile(hFile, buffer, dwSize, &dwReadSize, NULL))
		{
			return dwSize == dwReadSize;
		}
	}
	return false;
}

bool lazydb::offset_read_safe(HANDLE hFile, wxt_index offset, char *buffer, DWORD dwSize)
{
	LARGE_INTEGER li_offset;
	li_offset.QuadPart = offset;
	if (::SetFilePointer(hFile, (long)li_offset.LowPart, &li_offset.HighPart, FILE_BEGIN) == li_offset.LowPart)
	{
		DWORD dwReadSize = 0;
		wx_lock(pl_read, hFile, file_lock::type::read, offset, dwSize);
		if (::ReadFile(hFile, buffer, dwSize, &dwReadSize, NULL))
		{
			wx_unlock(pl_read);
			return dwSize == dwReadSize;
		}
		wx_unlock(pl_read);
	}
	return false;
}

void lazydb::find(const char *key, wxt_klength &klength_real, wxt_children &index)
{
	bool exists = false;

#ifndef lazydb_disable_memory_indexes
	memory_indexes::place place = { 0, 0 };
	if (m_pIndexes && m_pIndexes->done)
	{
		wxt_klength height_tmp = 0;
		wxt_children index_tmp = 0;
		if (m_pIndexes->indexes.try_get(key, height_tmp, index_tmp))
		{
			place = { height_tmp, index_tmp };
			klength_real = height_tmp;
			index = index_tmp;
		}
	}
#endif

#ifndef lazydb_one_process
	lazydb_node *node = nullptr;
	wxt_hash key_hash = (wxt_hash)bkdr_hash(key);
	wxt_klength key_length = (wxt_klength)strlen(key),
		key_length_real = key_length << 1;
	for (wxt_klength i = klength_real - 1; i < key_length_real; ++i)
	{
		if (index == 0) break;//不存在子节点，说明到了叶子节点

		unsigned long key_real = 0xfUL & (i & 1UL ? key[i >> 1] : (key[i >> 1] >> 4));
		//sizeof(head) + (index - 1) * sizeof(lazydb_children) + realkey * sizeof(lazydb_node),index是从1开始的
		__int64	node_offset = (__int64)DISK_BLOCK_SIZE + ((index - 1) << 9) + (key_real << 5);

		DWORD dwReadSize = 0;
		wx_lock(pl_iread, m_hIlazydb, file_lock::type::read, node_offset, sizeof(lazydb_node));
		if (node_read(node_offset, &node, dwReadSize))
		{
			wx_unlock(pl_iread);
			if (node->klength == 0)
			{
				break;
			}
			else if (klength_real == key_length_real)
			{
				//因为已经走到头了，key的每一个字符都已经遍历过，那么如果两个值长度相等，
				//便意味着每个字符都相等，即字符串相等：长度相等 <=> 字符串相等
				//反之（逆否） ： 长度不相等 <=> 字符串不相等
				if (node->klength == key_length)
				{
					exists = true;
#ifndef lazydb_disable_memory_indexes
					if (m_pIndexes && m_pIndexes->done && (place.height != klength_real || place.index != index))
					{
						update_indexex(key, { klength_real, index });
					}
#endif
				}
				break;
			}
			else
			{
				//先判断长度和hash是否相等
				if (node->klength == key_length && node->hash == key_hash)
				{
					char *kbuffer = new char[node->klength + 1]();
					if (offset_read_safe(m_hKlazydb, node->key, kbuffer, node->klength))
					{
						if (strcmp(key, kbuffer) == 0)
						{
							exists = true;
#ifndef lazydb_disable_memory_indexes
							if (m_pIndexes && m_pIndexes->done && (place.height != klength_real || place.index != index))
							{
								update_indexex(key, { klength_real, index });
							}
#endif
						}
					}
					delete[] kbuffer;
					if (exists)
					{
						break;
					}
				}
				if (node->children == 0)
				{
					break;
				}
				else
				{
					index = node->children;
				}
			}
		}
		else
		{
			wx_unlock(pl_iread);
			break;
		}
		++klength_real;
	}
	if (node)
	{
		delete node;
	}
#endif
}

//节点驱赶
//head：头信息；length_real：驱赶到哪一层；index：驱赶到哪个节点群；key：键；value：值
bool lazydb::occupy_to(wxt_klength length_real, wxt_children index, const char *key, const char *value)
{
	lazydb_node *node = nullptr;
	lazydb_children *children = nullptr;
	wxt_hash key_hash = (wxt_hash)bkdr_hash(key);
	wxt_klength key_length = (wxt_klength)strlen(key),
		key_length_real = key_length << 1;
	wxt_vlength value_length = (wxt_vlength)strlen(value);
	bool b_had_handle_head = false;

    LARGE_INTEGER li_filesize;
    DWORD dwHeight = 0;
    li_filesize.LowPart = ::GetFileSize(m_hIlazydb, &dwHeight);
    li_filesize.HighPart = dwHeight;

	for (wxt_klength i = length_real - 1; i < key_length_real; ++i)
    {
        lazydb_node *current_node = nullptr;

		unsigned long key_real = 0xfUL & (i & 1UL ? key[i >> 1] : (key[i >> 1] >> 4));
		__int64	children_offset = (__int64)DISK_BLOCK_SIZE + ((index - 1) << 9);//index * sizeof(lazydb_children),index是从1开始的
		//sizeof(head) + (index - 1) * sizeof(lazydb_children) + realkey * sizeof(lazydb_node),index是从1开始的
		__int64	node_offset = children_offset + (key_real << 5);
		DWORD dwReadSize = 0;
		current_node = nullptr;
#ifndef lazydb_one_process
		file_lock pl_iwrite;
#endif
        bool b_need_create_children = node_offset >= li_filesize.QuadPart;
		if (b_need_create_children)
		{
			//如果节点群尚不存在，则锁整个节点群，因为下面要创建这个节点群
			do_wx_lock(pl_iwrite, m_hIlazydb, file_lock::type::write, children_offset, sizeof(lazydb_children));
			if (children_read(children_offset, &children, dwReadSize))
			{
				current_node = &(children->nodes[key_real]);
			}
		}
		else
		{
			//如果节点群尚存在，则只锁这个节点，下面更新就可以
			do_wx_lock(pl_iwrite, m_hIlazydb, file_lock::type::write, node_offset, sizeof(lazydb_node));
			if (node_read(node_offset, &node, dwReadSize))
			{
				current_node = node;
			}
		}
		if (current_node)
		{
			bool b_need_handle_head = !b_had_handle_head;
			b_had_handle_head = false;

			if (current_node->klength == 0)
			{
				if (b_need_handle_head)
				{
					//校验guid并修改头
					ilazydb_head head = { 0 };
					wx_lock(pl_iwhead, m_hIlazydb, file_lock::type::write, 0, sizeof(ilazydb_head));
					if (!ihead_read(&head))//因为要改变头，所以重现读一次最新的数据再写入
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_iwhead);
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
					if (m_guid != head.guid)
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_iwhead);
						reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
					if (dwReadSize == 0 && head.number == 0)
					{
						++head.number;
					}
					++head.count;
					ihead_write(head);
					wx_unlock(pl_iwhead);
				}
				else
				{
					//校验guid
					ilazydb_head head = { 0 };
					wx_lock(pl_irhead, m_hIlazydb, file_lock::type::read, 0, sizeof(ilazydb_head));
					if (!ihead_read(&head))
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_irhead);
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
					if (m_guid != head.guid)
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_irhead);
						reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
					wx_unlock(pl_irhead);
				}

				current_node->klength = key_length;
				current_node->vlength = value_length;
				current_node->children = 0;//当前节点创建时，子节点群位置不确定，需要到真正创建子节点的时候才分配
				current_node->hash = (wxt_hash)bkdr_hash(key);
				current_node->key = 0;
				current_node->value = 0;

				if (!append_write_safe(m_hKlazydb, key, key_length, current_node->key))
				{
					wx_unlock(pl_iwrite);
					if (children)
					{
						delete children;
					}
					if (node)
					{
						delete node;
					}
					return false;
				}
				if (!append_write_safe(m_hVlazydb, value, value_length, current_node->value))
				{
					wx_unlock(pl_iwrite);
					if (children)
					{
						delete children;
					}
					if (node)
					{
						delete node;
					}
					return false;
				}

				bool b_write_success = false;
				if (b_need_create_children)
				{
					b_write_success = children_write(children_offset, children);
				}
				else
				{
					b_write_success = node_write(node_offset, current_node);
				}
				if (!b_write_success)
				{
					wx_unlock(pl_iwrite);
					if (children)
					{
						delete children;
					}
					if (node)
					{
						delete node;
					}
					return false;
				}
				wx_unlock(pl_iwrite);
#ifndef lazydb_disable_memory_indexes
				update_indexex(key, { length_real, index });
#endif
				break;
			}
			else if (length_real == key_length_real)
			{
				//校验guid
				ilazydb_head head = { 0 };
				wx_lock(pl_irhead, m_hIlazydb, file_lock::type::read, 0, sizeof(ilazydb_head));
				if (!ihead_read(&head))
				{
					wx_unlock(pl_iwrite);
					wx_unlock(pl_irhead);
					if (children)
					{
						delete children;
					}
					if (node)
					{
						delete node;
					}
					return false;
				}
				if (m_guid != head.guid)
				{
					wx_unlock(pl_iwrite);
					wx_unlock(pl_irhead);
					reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
					if (children)
					{
						delete children;
					}
					if (node)
					{
						delete node;
					}
					return false;
				}
				wx_unlock(pl_irhead);

				lazydb_node old_node = *current_node;
				//还要判断是不是同一个值：若是，则直接覆盖值；否，则需要进行驱赶。
				bool boccupy = false;
				char *occupy_kbuffer = nullptr;
				char *occupy_vbuffer = nullptr;
				if (current_node->klength != key_length)
				{
					//因为已经走到头了，key的每一个字符都已经遍历过，那么如果两个值长度相等，
					//便意味着每个字符都相等，即字符串相等：长度相等 <=> 字符串相等
					//反之（逆否） ： 长度不相等 <=> 字符串不相等;
					occupy_vbuffer = new char[current_node->vlength + 1]();
					if (offset_read_safe(m_hVlazydb, current_node->value, occupy_vbuffer, current_node->vlength))
					{
					}
					if (occupy_vbuffer[0])
					{
						boccupy = true;
						//如果value值不为空才驱赶，如果为空则直接抹杀掉
						occupy_kbuffer = new char[current_node->klength + 1]();
						if (offset_read_safe(m_hKlazydb, current_node->key, occupy_kbuffer, current_node->klength))
						{
						}
					}
				}

				current_node->klength = key_length;
				//current_node->children;已经有了，或上面已经分配，不需要重新指定
				current_node->hash = (wxt_hash)bkdr_hash(key);

				if (boccupy)
				{
					current_node->key = 0;
					current_node->value = 0;
					current_node->vlength = value_length;
					//若发生了驱赶或者，则额外开辟空间，写到文件尾
					if (!append_write_safe(m_hKlazydb, key, key_length, current_node->key))
					{
						wx_unlock(pl_iwrite);
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						if (occupy_kbuffer) delete[] occupy_kbuffer;
						if (occupy_vbuffer) delete[] occupy_vbuffer;
						return false;
					}
					if (!append_write_safe(m_hVlazydb, value, value_length, current_node->value))
					{
						wx_unlock(pl_iwrite);
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						if (occupy_kbuffer) delete[] occupy_kbuffer;
						if (occupy_vbuffer) delete[] occupy_vbuffer;
						return false;
					}
				}
				else
				{
					//若未发生驱赶，则是覆盖值，因此key不需要修改
					if (old_node.vlength < value_length)
					{
						current_node->value = 0;
						current_node->vlength = value_length;
						//当前的位置不够写，则额外开辟空间，写到文件尾
						if (!append_write_safe(m_hVlazydb, value, value_length, current_node->value))
						{
							wx_unlock(pl_iwrite);
							if (children)
							{
								delete children;
							}
							if (node)
							{
								delete node;
							}
							if (occupy_kbuffer) delete[] occupy_kbuffer;
							if (occupy_vbuffer) delete[] occupy_vbuffer;
							return false;
						}
					}
					else
					{
						//当前的位置够写，则直接写入
						if (current_node->vlength != 0)
						{
							current_node->value = old_node.value;
							char *vbuffer = new char[current_node->vlength]();
							memcpy(vbuffer, value, value_length);
							if (!offset_write_safe(m_hVlazydb, current_node->value, vbuffer, current_node->vlength))
							{
								wx_unlock(pl_iwrite);
								delete[] vbuffer;
								if (children)
								{
									delete children;
								}
								if (node)
								{
									delete node;
								}
								if (occupy_kbuffer) delete[] occupy_kbuffer;
								if (occupy_vbuffer) delete[] occupy_vbuffer;
								return false;
							}
							delete[] vbuffer;
						}
					}
				}

				if (boccupy && current_node->children == 0)
				{
					//如果是叶子节点，那就先帮被驱赶的值申请好子节点群
					//校验guid并修改头
					ilazydb_head head = { 0 };
					wx_lock(pl_iwhead, m_hIlazydb, file_lock::type::write, 0, sizeof(ilazydb_head));
					if (!ihead_read(&head))//因为要改变头，所以重现读一次最新的数据再写入
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_iwhead);
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						if (occupy_kbuffer) delete[] occupy_kbuffer;
						if (occupy_vbuffer) delete[] occupy_vbuffer;
						return false;
					}
					if (m_guid != head.guid)
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_iwhead);
						reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						if (occupy_kbuffer) delete[] occupy_kbuffer;
						if (occupy_vbuffer) delete[] occupy_vbuffer;
						return false;
					}
					++head.number;
					ihead_write(head);
					wx_unlock(pl_iwhead);
					current_node->children = head.number;
				}

				bool b_write_success = false;
				if (b_need_create_children)
				{
					b_write_success = children_write(children_offset, children);
				}
				else
				{
					b_write_success = node_write(node_offset, current_node);
				}
				if (!b_write_success)
				{
					wx_unlock(pl_iwrite);
					if (children)
					{
						delete children;
					}
					if (node)
					{
						delete node;
					}
					if (occupy_kbuffer) delete[] occupy_kbuffer;
					if (occupy_vbuffer) delete[] occupy_vbuffer;
					return false;
				}
				wx_unlock(pl_iwrite);
#ifndef lazydb_disable_memory_indexes
				update_indexex(key, { length_real, index });
#endif
				if (boccupy)
				{
					if (current_node->children == 0)
					{
						occupy_to(length_real, index, occupy_kbuffer, occupy_vbuffer);
					}
					else
					{
						occupy_to(length_real + 1, current_node->children, occupy_kbuffer, occupy_vbuffer);
					}
				}
				if (occupy_kbuffer) delete[] occupy_kbuffer;
				if (occupy_vbuffer) delete[] occupy_vbuffer;

				break;
			}
			else
			{
				//先判断长度和hash是否相等
				if (current_node->klength == key_length && current_node->hash == key_hash)
				{
					//校验guid
					ilazydb_head head = { 0 };
					wx_lock(pl_irhead, m_hIlazydb, file_lock::type::read, 0, sizeof(ilazydb_head));
					if (!ihead_read(&head))
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_irhead);
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
					if (m_guid != head.guid)
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_irhead);
						reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
					wx_unlock(pl_irhead);

					bool exists = false;
					char *kbuffer = new char[current_node->klength + 1]();
					if (offset_read_safe(m_hKlazydb, current_node->key, kbuffer, current_node->klength))
					{
						if (strcmp(key, kbuffer) == 0)
						{
							exists = true;
							//是覆盖值，因此key不需要修改
							if (current_node->vlength < value_length)
							{
								//其他的内容未变化或上面的步骤中已经修改了
								current_node->vlength = value_length;//== max(current_node->vlength, value_length);

								//当前的位置不够写，则额外开辟空间，写到文件尾
								if (!append_write_safe(m_hVlazydb, value, value_length, current_node->value))
								{
									wx_unlock(pl_iwrite);
									if (children)
									{
										delete children;
									}
									if (node)
									{
										delete node;
									}
									return false;
								}
							}
							else
							{
								//其他的内容未变化或上面的步骤中已经修改了
								//current_node->vlength = max(current_node->vlength, value_length);因为vlength更大，所以不需要修改

								//当前的位置够写，则直接写入
								if (current_node->vlength != 0)
								{
									char *vbuffer = new char[current_node->vlength]();
									memcpy(vbuffer, value, value_length);
									if (!offset_write_safe(m_hVlazydb, current_node->value, vbuffer, current_node->vlength))
									{
										wx_unlock(pl_iwrite);
										delete[] vbuffer;
										if (children)
										{
											delete children;
										}
										if (node)
										{
											delete node;
										}
										return false;
									}
									delete[] vbuffer;
								}
							}
						}
					}
					delete[] kbuffer;
					if (exists)
					{
						bool b_write_success = false;
						if (b_need_create_children)
						{
							b_write_success = children_write(children_offset, children);
						}
						else
						{
							b_write_success = node_write(node_offset, current_node);
						}
						if (!b_write_success)
						{
							wx_unlock(pl_iwrite);
							if (children)
							{
								delete children;
							}
							if (node)
							{
								delete node;
							}
							return false;
						}
						wx_unlock(pl_iwrite);
#ifndef lazydb_disable_memory_indexes
						update_indexex(key, { length_real, index });
#endif
						break;
					}
				}
				if (current_node->children == 0)
				{
					//向这个节点的子节点插入值，先设置其子节点群，下一个循环再设置子节点的数据
					ilazydb_head head = { 0 };
					wx_lock(pl_iwhead, m_hIlazydb, file_lock::type::write, 0, sizeof(ilazydb_head));
					if (!ihead_read(&head))//因为要改变头，所以重现读一次最新的数据再写入
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_iwhead);
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
					if (m_guid != head.guid)
					{
						wx_unlock(pl_iwrite);
						wx_unlock(pl_iwhead);
						reopen();//需要重新打开文件，因为trim只重写了头，其他部分得重新打开后读取才会变化
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
					++head.number;
					++head.count;
					ihead_write(head);
					wx_unlock(pl_iwhead);

					b_had_handle_head = true;
					current_node->children = head.number;//number已经加1了

					bool b_write_success = false;
					if (b_need_create_children)
					{
						b_write_success = children_write(children_offset, children);
					}
					else
					{
						b_write_success = node_write(node_offset, current_node);
					}
					if (!b_write_success)
					{
						wx_unlock(pl_iwrite);
						if (children)
						{
							delete children;
						}
						if (node)
						{
							delete node;
						}
						return false;
					}
#ifndef lazydb_disable_memory_indexes
					update_indexex(key, { length_real, index });
#endif
				}
				index = current_node->children;
			}
		}
		else
		{
			wx_unlock(pl_iwrite);
			if (children)
			{
				delete children;
			}
			if (node)
			{
				delete node;
			}
			return false;
		}
		wx_unlock(pl_iwrite);
		++length_real;
	}
	bool bresult = false;
	if (children)
	{
		delete children;
		bresult = true;
	}
	if (node)
	{
		delete node;
		bresult = true;
	}
	return bresult;
}

long lazydb::prefix_all_descendant(lazydb_children *children, lazydb_prefix_get_callback callback, void *userdata, bool &bbreak)
{
	lazydb_children *next_children = nullptr;
	int count = 0;
	for (int i = 0; i < 16; ++i)
	{
		lazydb_node *node = &(children->nodes[i]);
		if (node->klength)
		{
			char *kbuffer = new char[node->klength + 1]();
			if (offset_read_safe(m_hKlazydb, node->key, kbuffer, node->klength))
			{
				char *vbuffer = new char[node->vlength + 1]();
				if (offset_read_safe(m_hVlazydb, node->value, vbuffer, node->vlength))
				{
					bbreak = callback(kbuffer, vbuffer, userdata);
					++count;
				}
				delete[] vbuffer;
			}
			delete[] kbuffer;

			if (node->children)
			{
				DWORD dwReadSize = 0;
				__int64	children_offset = (__int64)DISK_BLOCK_SIZE + ((node->children - 1) << 9);//index * sizeof(lazydb_children),index是从1开始的
				wx_lock(pl_iread, m_hIlazydb, file_lock::type::read, children_offset, sizeof(lazydb_children));
				if (children_read(children_offset, &next_children, dwReadSize))
				{
					wx_unlock(pl_iread);
					count += prefix_all_descendant(next_children, callback, userdata, bbreak);
				}
				else
				{
					wx_unlock(pl_iread);
				}
			}
		}
		if (bbreak) break;
	}
	if (next_children)
	{
		delete next_children;
	}
	return count;
}

#ifndef lazydb_disable_memory_indexes
//创建索引的单步
void lazydb::create_indexes_step(wxt_klength length_real, wxt_children index, lazydb_indexes_step_callback callback, void *userdata)
{
	lazydb_children *children = nullptr;
	DWORD dwReadSize = 0;
	__int64	children_offset = (__int64)DISK_BLOCK_SIZE + ((index - 1) << 9);//index * sizeof(lazydb_children),index是从1开始的
	wx_lock(pl_iread, m_hIlazydb, file_lock::type::read, children_offset, sizeof(lazydb_children));
	if (children_read(children_offset, &children, dwReadSize))
	{
		wx_unlock(pl_iread);
		for (int i = 0; i < 16; ++i)
		{
			lazydb_node *node = &children->nodes[i];
			if (node->klength)
			{
				char *kbuffer = new char[node->klength + 1]();
				if (offset_read_safe(m_hKlazydb, node->key, kbuffer, node->klength))
				{
					callback(kbuffer, { length_real, index }, userdata);
				}
				delete[] kbuffer;

				if (node->children != 0)
				{
					create_indexes_step(length_real + 1, node->children, callback, userdata);
				}
			}
		}
	}
	else
	{
		wx_unlock(pl_iread);
	}
	if (children)
	{
		delete children;
	}
}

//更新索引
void lazydb::update_indexex(const char *key, const memory_indexes::place &place)
{
	if (m_pIndexes && m_pIndexexList)
	{
		m_pIndexexList->array[m_pIndexexList->index].push_back({ key, place });
	}
}
#endif

//计算字符串的hash
unsigned long lazydb::bkdr_hash(const char *str)
{
	register unsigned long hash = 0;
	while (unsigned long ch = (unsigned long)*str++)
	{
		hash = hash * 131 + ch;//也可以乘以31、131、1313、13131、131313.. 
	}
	return hash;
}
