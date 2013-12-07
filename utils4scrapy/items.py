# -*- coding: utf-8 -*-

from scrapy.item import Item, Field


class UserItem(Item):
    id = Field()
    name = Field()
    gender = Field()
    province = Field()
    city = Field()
    location = Field()
    description = Field()
    verified = Field()
    verified_reason = Field()
    verified_type = Field()
    followers_count = Field()  # 粉丝数
    statuses_count = Field()
    friends_count = Field()  # 关注数
    profile_image_url = Field()
    bi_followers_count = Field()  # 互粉数
    followers = Field()  # just ids
    friends = Field()  # just ids
    created_at = Field()

    active = Field()
    first_in = Field()
    last_modify = Field()

    RESP_ITER_KEYS = ['id', 'name', 'gender', 'province', 'city', 'location',
                      'description', 'verified', 'followers_count',
                      'statuses_count', 'friends_count', 'profile_image_url',
                      'bi_followers_count', 'verified_reason', 'verified_type', 'created_at']

    PIPED_UPDATE_KEYS = ['name', 'gender', 'province', 'city', 'location',
                         'description', 'verified', 'followers_count',
                         'statuses_count', 'friends_count', 'profile_image_url',
                         'bi_followers_count', 'verified', 'verified_reason', 'verified_type', 'created_at']

    def __init__(self):
        """
        >>> a = UserItem()
        >>> a
        {'followers': [], 'friends': []}
        >>> a.to_dict()
        {'followers': [], 'friends': []}
        """

        super(UserItem, self).__init__()
        default_empty_arr_keys = ['followers', 'friends']
        for key in default_empty_arr_keys:
            self.setdefault(key, [])

        self.setdefault('active', False)

    def to_dict(self):
        d = {}
        for k, v in self.items():
            if isinstance(v, (UserItem, WeiboItem)):
                d[k] = v.to_dict()
            else:
                d[k] = v
        return d


class WeiboItem(Item):
    created_at = Field()
    timestamp = Field()
    id = Field()
    mid = Field()
    text = Field()
    source = Field()
    reposts_count = Field()
    comments_count = Field()
    attitudes_count = Field()
    bmiddle_pic = Field()
    original_pic = Field()
    geo = Field()
    #  预留字段，不写入数据
    urls = Field()
    hashtags = Field()
    emotions = Field()
    at_users = Field()
    repost_users = Field()
    #
    user = Field()  # 信息可能过期
    retweeted_status = Field()
    reposts = Field()  # just ids
    comments = Field()  # just ids

    first_in = Field()
    last_modify = Field()

    RESP_ITER_KEYS = ['created_at', 'id', 'mid', 'text', 'source', 'reposts_count',
                      'comments_count', 'attitudes_count', 'geo', 'bmiddle_pic', 'original_pic']
    PIPED_UPDATE_KEYS = ['reposts_count', 'comments_count', 'attitudes_count']

    def __init__(self):
        super(WeiboItem, self).__init__()
        default_empty_arr_keys = ['reposts', 'comments']
        for key in default_empty_arr_keys:
            self.setdefault(key, [])

    def to_dict(self):
        d = {}
        for k, v in self.items():
            if isinstance(v, (UserItem, WeiboItem)):
                d[k] = v.to_dict()
            else:
                d[k] = v

            """
            elif type(v) == list:
                d[k] = []
                for vv in v:
                    d[k].append(vv.to_dict())
            else:
                d[k] = v
            """

        return d


class WeiboItem_v1(Item):
    id = Field() # 16位微博ID
    mid = Field() # 16位微博ID
    created_at = Field()
    timestamp = Field() # created_at字段转化而来的时间戳
    text = Field()
    source = Field()
    favorited = Field()
    truncated = Field()
    in_reply_to_status_id = Field()
    in_reply_to_user_id = Field()
    in_reply_to_screen_name = Field()
    pic_urls = Field() # thumbnail_pic list
    thumbnail_pic = Field()
    geo = Field()
    user = Field()  # just uid
    retweeted_status = Field() # just mid
    #  预留字段，不写入数据
    urls = Field()
    hashtags = Field()
    emotions = Field()
    at_users = Field()
    repost_users = Field()
    #
    reposts = Field()  # just mids
    comments = Field()  # just ids
    # user_timeline_v2
    idstr = Field() # 字符串型微博ID
    mlevel = Field()
    reposts_count = Field()
    comments_count = Field()
    attitudes_count = Field()
    visible = Field()
    ad = Field() # 微博流内的推广微博ID
    # 自定义字段
    first_in = Field()
    last_modify = Field()

    RESP_ITER_KEYS = ['created_at', 'id', 'mid', 'text', 'source', 'favorited', 'truncated', 
                      'in_reply_to_status_id', 'in_reply_to_user_id', 'in_reply_to_screen_name', 
                      'pic_urls', 'thumbnail_pic', 'geo']

    PIPED_UPDATE_KEYS = ['favorited', 'truncated']

    def __init__(self):
        super(WeiboItem_v1, self).__init__()
        default_empty_arr_keys = ['reposts', 'comments']
        for key in default_empty_arr_keys:
            self.setdefault(key, [])

    def to_dict(self):
        d = {}
        for k, v in self.items():
            if isinstance(v, (UserItem_v1, WeiboItem_v1)):
                d[k] = v.to_dict()
            else:
                d[k] = v

        return d


class UserItem_v1(Item):
    created_at = Field()
    timestamp = Field() # created_at字段转化而来的时间戳
    id = Field() # 用户UID
    name = Field()
    gender = Field()
    province = Field()
    city = Field()
    location = Field()
    description = Field()
    url = Field() # 用户博客地址
    domain = Field() # 用户个性化URL
    geo_enabled = Field() # 是否允许标识用户的地理位置
    verified = Field() # 加V标示，是否微博认证用户
    verified_type = Field() # 用户认证类型
    followers_count = Field()  # 粉丝数
    statuses_count = Field() # 微博数
    friends_count = Field()  # 关注数
    favourites_count = Field() # 收藏数
    profile_image_url = Field()
    allow_all_act_msg = Field()
    # user_timeline_v2
    idstr = Field() # 字符串型的用户UID
    profile_url = Field() # 用户的微博统一URL地址
    weihao = Field() # 用户的微号
    verified_reason = Field() # 认证原因
    allow_all_comment = Field() # 是否允许所有人对我的微博进行评论
    online_status = Field() # 用户的在线状态，0：不在线、1：在线
    bi_followers_count = Field()
    lang = Field() # 用户当前的语言版本，zh-cn：简体中文，zh-tw：繁体中文，en：英语
    #
    followers = Field()  # just uids
    friends = Field()  # just uids
    # 自定义字段
    first_in = Field()
    last_modify = Field()

    RESP_ITER_KEYS = ['id', 'name', 'gender', 'province', 'city', 'location', 'url', 'domain',
                      'geo_enabled', 'verified', 'verified_type', 'description', 
                      'followers_count', 'statuses_count', 'friends_count', 'favourites_count',
                      'profile_image_url', 'allow_all_act_msg', 'created_at']

    PIPED_UPDATE_KEYS = ['name', 'gender', 'province', 'city', 'location', 'url', 'domain',
                         'geo_enabled', 'verified', 'verified_type', 'description',
                         'followers_count', 'statuses_count', 'friends_count', 'favourites_count',
                         'profile_image_url', 'allow_all_act_msg', 'created_at']

    def __init__(self):
        """
        >>> a = UserItem()
        >>> a
        {'followers': [], 'friends': []}
        >>> a.to_dict()
        {'followers': [], 'friends': []}
        """

        super(UserItem_v1, self).__init__()
        default_empty_arr_keys = ['followers', 'friends']
        for key in default_empty_arr_keys:
            self.setdefault(key, [])

    def to_dict(self):
        d = {}
        for k, v in self.items():
            if isinstance(v, (UserItem_v1, WeiboItem_v1)):
                d[k] = v.to_dict()
            else:
                d[k] = v
        return d