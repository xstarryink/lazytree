#pragma once
#include<type_traits>
#include<bits/unique_ptr.h>
#include<bits/move.h>
#include<bit>
#include<limits>
#include<cstddef>
namespace{struct EmptyClass{};}
template<typename key_t,typename value_t=EmptyClass> requires std::is_integral_v<key_t>
class LazyTree
{
	struct Node;
	using ptrNode=std::unique_ptr<Node>;
	using ukey_t=std::make_unsigned_t<key_t>;
	static inline int highbit(key_t value)
	{
		return std::numeric_limits<key_t>::digits-std::__countl_zero(ukey_t(value))-1;
	}
	struct Node:value_t
	{
		key_t key;
		ptrNode son[2];
		Node(key_t key)
		:value_t(),key{key},son{}{}
	};
	static constexpr key_t invalidKey{};
	ptrNode root;
	std::size_t m_size;
	Node *HalfwayInsert(const key_t key,key_t mask,ptrNode *cur_node)
	{
		for(;mask&&(*cur_node)&&((*cur_node)->key!=invalidKey)&&((*cur_node)->key!=key);mask>>=1)
			cur_node=(*cur_node)->son+static_cast<bool>(key&mask);
		++m_size;
		if(!(*cur_node))
		{
//			found an empty node on the way home
//			get lazy
			cur_node->reset(new Node(key));
			return cur_node->get();
		}
		else if(((*cur_node)->key)==invalidKey)
		{
			(*cur_node)->key=key;
			return cur_node->get();
		}
		else if((*cur_node)->key!=key)
		{
//			this is home and the node is occupied
//			expel the occupier
			key_t occupier=(*cur_node)->key;
			(*cur_node)->key=key;
			dynamic_cast<value_t&>(*HalfwayInsert(occupier,key_t(1)<<highbit(occupier)>>highbit(key)>>1,cur_node))=std::move(dynamic_cast<value_t&>(**cur_node));
			return cur_node->get();
		}
		else
		{
//			key already exist
			--m_size;
			throw "Key already exist";
		}
	}
	void eraseNode(ptrNode *node)
	{
		if(((*node)->son[0])||((*node)->son[1]))
			(*node)->key=invalidKey;
		else
			node->reset();
	}
public:
	LazyTree()noexcept=default;
	LazyTree(const LazyTree&)=delete;
	LazyTree(LazyTree &&b)noexcept
	:root(b.root.release()),m_size{b.m_size}{}
	LazyTree operator=(const LazyTree&)noexcept=delete;
	LazyTree operator=(LazyTree &&b)noexcept
	{
		root.reset(b.root.release());
		m_size=b.m_size;
	}
	~LazyTree()=default;
	void clear()
	{
		m_size=0;
		root.reset(nullptr);
	}
	value_t& insert(key_t key)
	{
		++key;
		return *HalfwayInsert(key,key_t(1)<<highbit(key)>>1,&root);
	}
	value_t& at(key_t key)
	{
		++key;
		ptrNode *cur_node=&root;
		Node *emptyNode{nullptr};
		key_t mask=1<<highbit(key)>>1;
		for(;mask&&(*cur_node)&&((*cur_node)->key)!=key;mask>>=1)
		{
			if((*cur_node)->key==invalidKey)
				emptyNode=cur_node->get();
			cur_node=(*cur_node)->son+static_cast<bool>(key&mask);
		}
		if((*cur_node)&&((*cur_node)->key==key))
		{
//			found
			if(emptyNode)
			{
				emptyNode->key=key;
				dynamic_cast<value_t&>(*emptyNode)=std::move(dynamic_cast<value_t&>(**cur_node));
				eraseNode(cur_node);
				return dynamic_cast<value_t&>(*emptyNode);
			}
			return **cur_node;
		}
		else
		{
			throw "key quiried not found";
		}
	}
	const value_t& at(key_t key) const
	{
		++key;
		ptrNode const *cur_node=&root;
		key_t mask=key_t(1)<<highbit(key)>>1;
		for(;mask&&(*cur_node)&&((*cur_node)->key)!=key;mask>>=1)
			cur_node=(*cur_node)->son+static_cast<bool>(key&mask);
		if((*cur_node)&&((*cur_node)->key==key))
			return dynamic_cast<value_t&>(**cur_node);
		else
			throw "key not found";
	}
	bool has(key_t key) const noexcept
	{
		try
		{
			at(key);
			return true;
		}
		catch(const char*)
		{
			return false;
		}
	}
	bool count(key_t key) const noexcept
	{
		try
		{
			at(key);
			return true;
		}
		catch(const char*)
		{
			return false;
		}
	}
	void erase(key_t key)
	{
		++key;
		ptrNode *cur_node=&root;
		key_t mask=key_t(1)<<highbit(key)>>1;
		for(;mask&&(*cur_node)&&((*cur_node)->key)!=key;mask>>=1)
			cur_node=(*cur_node)->son+static_cast<bool>(key&mask);
		if((*cur_node)&&(*cur_node)->key==key)
			eraseNode(cur_node);
		else
			throw "key not found";
		--m_size;
	}
	std::size_t size() const noexcept
	{
		return m_size;
	}
	bool empty() const noexcept
	{
		return m_size==0;
	}
	std::size_t max_size() const noexcept
	{
		return std::numeric_limits<size_t>::max();
	}
	void swap(LazyTree &b) noexcept
	{
		std::swap(root,b.root);
		std::swap(m_size,b.m_size);
	}

};
